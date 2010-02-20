#|
exec sbcl --noinform --load $0 --end-toplevel-options "$@"
|#
;; -*- mode: lisp; mode: outline-minor; -*-

;; Copyright (C) 2010 Michael Zeller

;;* Commentary
;;
;; This file implements a simple database that is influenced by the SimpleDB project @ MIT
;;

;;* Tasks

;; - Add unit tests

;; - Add conditions/errors on bad input

;; - Package code

;; - Keep tuples sorted on page (only tiny overhead when
;;   inserting/deleting)

;; - Fix table ids, currently they are not completely unique (based on
;;   filename)

;; - Add table name and column name information to the catalog

;; - Create a reader for getting the table-number for a file from the
;;   file-name, this should encapsulate sxhash, and I can easily try
;;   my idea below.

;; - Allow multiple indexes per table, need to iterate over a list of
;;   indexes in add-tuple-to-page and delete-tuple-from-page.

;; - There is the issue of DUPLICATES, and how they should best be
;;   handled. Currently duplicates are removed together, since there
;;   is no way to distinguish between the two.  Similarly, all code
;;   below assumes that if a tuple is being requested to be deleted
;;   from an index, then all tuples with that key in the heap-file
;;   have been deleted (which could not be the case if (200 1) (200 2)
;;   are entries, and (200 1) is deleted and the key-field is 0). The
;;   only way around this is to automatically make a unique field and
;;   store this for all tuples, or to use slot numbers.  The unique
;;   field idea is probably the best option.

;; - Add update-tuple, and update indexes if the page-number changes
;;   for a tuple (hopefully this is rare)

;; - INT type needs to write arbitrary number of bytes, since Lisp can
;;   store arbitrary precision numbers. Currently the max INT is set
;;   as a parameter.

;;* Ideas

;; - Can I remove sxhash and just hash the file object itself?
;;   Wouldn't that make more sense record-id would then contain a
;;   pointer to the heap-file itself and no need for a catalog (except
;;   for looking up the names and column names of tables)

;;* Code

(require 'cl-ppcre)
(require 'flexi-streams)

;;** some helper functions

(defvar *debug* t
  "Flag for enabling debugging information to *standard-output*")

(defmacro write-debug (&rest args)
  (if *debug*
      `(format t ,@args)))

;; read-null-terminated-ascii and write-null-terminated-ascii from
;; "Practical Common Lisp"

(defconstant +null+ (code-char 0))

(defun read-null-terminated-ascii (in)
  (let ((value 
         (with-output-to-string (s)
           (loop for char = (let ((byte-value (read-byte in nil)))
                              (if byte-value
                                  (code-char byte-value)
                                  +null+))
              until (char= char +null+) do (write-char char s)))))
    (if (not (string= value "")) value)))

(defun write-null-terminated-ascii (string out)
  (loop for char across string
       do (write-byte (char-code char) out))
  (write-byte (char-code +null+) out))

(defun read-offset (in)
  (let ((value 0)
        (offset 0))
    (loop repeat *offset-size* do
         (progn 
           (setf (ldb (byte 8 offset) value) (read-byte in nil))
           (incf offset 8)))
    value))

(defun write-offset (value out)
  (let ((offset 0))
    (loop repeat *offset-size* do 
         (progn
           (write-byte (ldb (byte 8 offset) value) out)
           (incf offset 8)))))

(defun read-int (in)
  (let ((*page-offset* *int-size*))
    (read-offset in)))

(defun write-int (value out)
  (let ((*page-offset* *int-size*))
    (write-offset value out)))

;;** tuple

(defun write-tuple (stream tuple type-descriptor)
  (mapcar #'(lambda (type)
              (case type
                (int (write-int (pop tuple) stream))
                (string (write-null-terminated-ascii (pop tuple) stream))
                (otherwise nil)))
          type-descriptor))

(defun read-tuple (stream type-descriptor)
  (mapcar #'(lambda (type)
              (case type
                (int (read-int stream))
                (string (read-null-terminated-ascii stream))
                (otherwise nil)))
          type-descriptor))

;;** heap-file

;; heap-file is a list of heap-pages for a single table

(defclass file ()
  ((file-name :initarg :file-name :accessor file-name)
   (type-descriptor :initarg :type-descriptor :accessor type-descriptor)
   (page-count :reader page-count)))

(defmethod initialize-instance :after ((file file) &key new?)
  (write-debug "Initializing file.~%")
  (when (and new? (probe-file (file-name file)))
    (delete-file (file-name file)))
  (with-open-file (in (file-name file)
                      :if-exists :supersede
                      :if-does-not-exist :create 
                      :element-type '(unsigned-byte 8))
    (let ((page-count (/ (file-length in) *page-size*)))
      (setf (slot-value file 'page-count) page-count)
      (write-debug "Found a file with ~a pages.~%" page-count)))
  (catalog-add-file (sxhash (file-name file)) file))

(defgeneric filter (file filter callback)
  (:documentation "Calls the callback on each tuple for which the filter is true."))

(defgeneric add-tuple (file tuple)
  (:documentation "Adds a tuple to an file. Returns dirtied pages."))

(defgeneric delete-tuple (file tuple)
  (:documentation "Deletes a tuple from a file. Returns dirtied pages."))

(defgeneric print-file (file)
  (:documentation "Prints the contents of a file to *standard-output*"))

(defun print-tuple (tuple type-descriptor record-id)
  (format t "~{~a~^, ~}~%"
          (mapcar #'(lambda (data type)
                      (case type
                        (int (format nil "~a" data))
                        (string (format nil "\"~a\"" data))
                        (otherwise nil)))
                  tuple type-descriptor)))

(defmethod print-file ((file file))
  (write-debug "Printing file.~%")
  (filter file nil #'print-tuple))

(defmethod filter ((file file) filter callback)
  (write-debug "Filtering file.~%")
  (with-slots (type-descriptor page-count) file
    (loop for page-number from 0 to (- page-count 1) do 
         (let ((page (read-page file page-number)))
           (filter-page page type-descriptor filter callback)))))

(defclass heap-file (file) 
  ((next-free-page-number :initform 0 :accessor next-free-page-number)))

;; adds a tuple to a file
;; note that read-page will create a new page when an invalid page-number is provided
(defmethod add-tuple ((file heap-file) tuple)
  (write-debug "Adding tuple: ~S~%" tuple)
  (let (dirtied)
    (with-slots (page-count type-descriptor next-free-page-number) file
      (loop for page-number from next-free-page-number to page-count do 
           (let ((page (add-tuple-to-page (read-page file page-number) tuple type-descriptor)))
             (if page (push page dirtied) (incf next-free-page-number)))
         until dirtied))
    dirtied))

(defmethod delete-tuple ((file heap-file) tuple)
  (write-debug "Deleting tuple: ~S~%" tuple)
  (let (dirtied)
    (with-slots (page-count type-descriptor) file
      (loop for page-number from 0 to (- page-count 1) do
           (let ((page (delete-tuple-from-page (read-page file page-number) tuple type-descriptor)))
             (if page (push page dirtied)))))
    dirtied))

;;** heap-page

;; heap-page is a list of tuples, of variable length, dumped to a file
;; - each tuple on the page has an offset in the header of size *offset-size*
;; - first offset stores end of tuples
;; - second offset stores beginning of tuples
;; - each offset after that is the end of the previous tuple
;; - the header is only as large as the value of the second offset
;; - the free space is equal to *first-offset*
;; - if there are N tuples, there are N + 1 offsets
;; - hence, *length-of-tuples-in-bytes* + (N+1) . *offset-size* = *total-size*
;; - repack the file on every write
(defclass record-id ()
  ((table-number :initarg :table-number :accessor table-number)
   (page-number :initarg :page-number :accessor page-number)))

(defclass page ()
  ((tuples :initarg :tuples :accessor tuples)
   (record-id :initarg :record-id :accessor record-id)
   (dirty? :initform nil :accessor dirty?)))

(defgeneric add-tuple-to-page (page tuple type-descriptor)
  (:documentation "Adds a tuple to a page. Returns page or nil if full."))

(defmethod add-tuple-to-page :around (page tuple type-descriptor)
  (let ((page (call-next-method)))
    (when page
      (with-slots (record-id) page
        (write-debug "Adding tuple to any index(s).~%")
        (let ((index (catalog-lookup-index (table-number record-id))))
          (when index
            (add-tuple-to-index index tuple record-id)))))
    page))

(defgeneric delete-tuple-from-page (page tuple type-descriptor)
  (:documentation "Deletes a tuple from a page. Returns page or nil if not found."))

(defmethod delete-tuple-from-page :around (page tuple type-descriptor)
  (when (call-next-method)
    (with-slots (record-id) page
      (write-debug "Deleting tuple to any index(s).~%")
      (let ((index (catalog-lookup-index (table-number record-id))))
        (when index
          (delete-tuple-from-index index tuple record-id))))))

(defgeneric write-page (file page)
  (:documentation "Outputs a representation of a page to disk of *page-size* bytes"))

(defgeneric read-page (file page-number)
  (:documentation "Reads a page from disk into a page datastructure"))

(defgeneric filter-page (page type-descriptor filter callback)
  (:documentation "Calls callback on each tuple in page."))

(defgeneric get-binary-page (page type-descriptor)
  (:documentation "Get a binary representation of a page that can be written to disk."))

(defmethod filter-page (page type-descriptor filter callback)
  (with-slots (tuples record-id) page
    (write-debug "Filtering page (~a).~%" (page-number record-id))
    (mapcar #'(lambda (tuple)
                (when (or (not filter)
                          (case (nth (field filter) type-descriptor)
                            ('int 
                             (case (op filter)
                               ('= (= (nth (field filter) tuple) (value filter)))
                               ('< (< (nth (field filter) tuple) (value filter)))
                               ('> (> (nth (field filter) tuple) (value filter)))
                               ('<= (<= (nth (field filter) tuple) (value filter)))
                               ('>= (>= (nth (field filter) tuple) (value filter)))))
                            ('string
                             (case (op filter)
                               ('= (string= (nth (field filter) tuple) (value filter)))
                               ('< (string< (nth (field filter) tuple) (value filter)))
                               ('> (string> (nth (field filter) tuple) (value filter)))
                               ('<= (string<= (nth (field filter) tuple) (value filter)))
                               ('>= (string>= (nth (field filter) tuple) (value filter)))))))
                  (funcall callback tuple type-descriptor record-id)))
            tuples)))

;; checks bufferpool for page before fetching page from file
(defmethod read-page :around ((file file) page-number)
  (write-debug "Fetching page (~a) from ~a.~%" page-number file)
  (let ((hash (+ (sxhash (file-name file)) page-number)))
    ;; lookup hash in bufferpool
    (let ((page (fetch-from-bufferpool hash)))
      (if page 
          (progn ;; page is in bufferpool
            (write-debug "Found page in bufferpool.~%")
            page)
          (let ((page (call-next-method)))
            ;; add page to bufferpool, but may need to evict
            (add-to-bufferpool hash page)
            page)))))

(defclass heap-page (page)
  ((next-page-number :initform 0 :reader next-page-number)
   (free-space :initarg :free-space :initform 0 :accessor free-space)))

(defgeneric (setf next-page-number) (value page)
  (:documentation "Sets the pointer to the next page in a chain."))

(defmethod (setf next-page-number) (value (page heap-page))
  (setf (slot-value page 'next-page-number) value)
  (setf (dirty? page) t)
  value)

;; returns the size (in bytes) of a tuple on disk
(defun tuple-size (tuple type-descriptor)
  (length (flexi-streams:with-output-to-sequence (stream :element-type '(unsigned-byte 8))
            (write-tuple stream tuple type-descriptor))))

(defmethod add-tuple-to-page ((page heap-page) tuple type-descriptor)
  (write-debug "Adding tuple to page: ~S~%" tuple)
  (with-slots (tuples free-space dirty?) page
    (let ((required-space (tuple-size tuple type-descriptor)))
      (write-debug "Required space: ~a~%" required-space)
      (cond ((< free-space required-space) 
             (write-debug "Not enough free space! (~a < ~a)~%" free-space required-space)
             nil)
            (t
             (push tuple tuples)
             (write-debug "Free space (before): ~a~%" free-space)
             (setf free-space (- free-space required-space))
             (write-debug "Free space (after): ~a~%" free-space)
             (setf dirty? t)
             page)))))

(defmethod delete-tuple-from-page ((page heap-page) tuple type-descriptor)
  (with-slots (tuples record-id free-space dirty?) page
    (let ((occurances (count tuple tuples :test 'equal)))
      (when (not (zerop occurances))
        (write-debug "Deleting ~a occurances of ~a from page ~a.~%" 
                occurances tuple (page-number record-id))
        (setf tuples (delete tuple tuples :test 'equal))
        (incf free-space (* (tuple-size tuple type-descriptor) occurances))
        (setf dirty? t)
        page))))
    
;; writes a heap-page to disk
;; assumes that the tuples fit on the page, need to check this when adding tuples!
(defmethod write-page ((file heap-file) (page heap-page))
  (with-slots (tuples record-id dirty?) page
    (write-debug "Writing page~%")
    (write-debug "Tuples: ~S~%" tuples)
    (with-slots (file-name type-descriptor) file
      (with-open-file (f file-name
                         :direction :output 
                         :if-exists :overwrite
                         :if-does-not-exist :create 
                         :element-type '(unsigned-byte 8))
        (let ((start (* (page-number record-id) *page-size*)))
          (write-debug "Writing to location: ~a~%" start)
          (file-position f start)
          (write-sequence (get-binary-page page type-descriptor) f)
          (setf dirty? nil))))))

(defmethod get-binary-page ((page heap-page) type-descriptor)
  (with-slots (tuples next-page-number) page
    (let (heap-data padding) 
      (setf padding
            (flexi-streams:with-output-to-sequence (out :element-type '(unsigned-byte 8))
              ;; write out 0 as page-offset, which allows linking of pages together
              (let ((*offset-size *page-offset-size*)) (write-offset next-page-number out))
              ;; write-out the number of tuples
              (write-offset (length tuples) out)
              ;; write-out the tuples to disk using the type to encode the field
              (mapcar #'(lambda (tuple)
                          (write-debug "Writing tuple: ~S (~a)~%" tuple type-descriptor)
                          (write-tuple out tuple type-descriptor)) tuples)
              ;; pad with zeros, looks good in hexdump
              (loop repeat 
                   (- *page-size* 
                      (length (setf heap-data (flexi-streams:get-output-stream-sequence out))))
                 do (write-byte 0 out))))
      (concatenate 'vector heap-data padding))))
  
;; reads a heap-page, given by its page-number, from a heap-file
(defmethod read-page ((file heap-file) page-number)
  (write-debug "Attempting to read a page.~%")
  (let ((heap-page (make-instance 'heap-page)))
    (with-slots (tuples record-id free-space dirty?) heap-page
      (with-slots (file-name type-descriptor page-count) file
        (with-open-file (f file-name
                           :element-type '(unsigned-byte 8))
          (setf record-id (make-instance 'record-id 
                                         :table-number (sxhash file-name)
                                         :page-number page-number))
          (cond 
            ((< page-number page-count) ;; page-number is valid
             (write-debug "Reading page (~a) from file~%" page-number)
             (let ((start (* (page-number record-id) *page-size*))
                   (buffer (make-array *page-size* :element-type '(unsigned-byte 8))))
               (file-position f start)
               (read-sequence buffer f)
               (flexi-streams:with-input-from-sequence (in buffer)
                 (let ((*offset-size* *page-offset-size*)) 
                   (setf (slot-value heap-page 'next-page-number) (read-offset in)))
                 (setf tuples (loop repeat (read-offset in) 
                                 collect (read-tuple in type-descriptor)))
                 (loop for x = (read-byte in nil) while x do (incf free-space)))))
            ((= page-number page-count)
             ;; page-number is invalid, but create a new page sequentially
             (write-debug "Creating a new page (~a)~%" page-number)
             (setf tuples nil)
             (setf dirty? t)
             (setf free-space (- *page-size* *offset-size* *page-offset-size*))
             (incf page-count))
            (t (error "abort: Page Index Of Out Bounds"))))))
    heap-page))
  
;;** filter

(defclass filter ()
  ((op :initarg :op :accessor op)
   (field :initarg :field :initform 0 :accessor field)
   (value :initarg :value :accessor value)))

;;** index-file

(defclass index ()
  ((source-file :initarg :source-file :initform (error "abort: Required") :reader source-file)
   (key-field :initarg :key-field :initform (error "abort: Required") :reader key-field)))

(defmethod initialize-instance :after ((index index) &key new?)
  (write-debug "Adding index to catalog.~%")
  (with-slots (source-file) index
    (catalog-add-index (sxhash (file-name source-file)) index)))
  
(defgeneric add-tuple-to-index (index tuple record-id)
  (:documentation "Adds an index entry for a tuple."))

(defgeneric delete-tuple-to-from-index (index tuple record-id)
  (:documentation "Adds an index entry for a tuple."))

(defclass index-file (index heap-file)
  ((bin-count :initarg :bin-count :initform *index-bin-count* :reader bin-count)
   next-free-page-number))

(defmethod initialize-instance :after ((file index-file) &key new?)
  (write-debug "Creating index-file~%")
  (with-slots (bin-count source-file key-field page-count next-free-page-number) file
    (setf (slot-value file 'type-descriptor)
          (list (nth key-field (type-descriptor source-file)) 'int))
    ;; create the pointers to the next page with free space for each bin
    (setf next-free-page-number (make-array bin-count))
    (loop for i from 0 below bin-count do (setf (aref next-free-page-number i) i))
    (when new?
      ;; create the bins
      (setf page-count bin-count)
      (loop for page-number from 0 below bin-count do (read-page file page-number))
      ;; add all of the tuples in source-file into the index
      (filter source-file nil
              #'(lambda (tuple type-descriptor record-id)
                  (add-tuple-to-index file tuple record-id))))))

(defmethod add-tuple-to-index ((file index-file) tuple record-id)
  (with-slots (page-count type-descriptor bin-count next-free-page-number key-field) file
    (let* ((key-value (nth key-field tuple))
           (bin-number (mod (sxhash key-value) bin-count))
           (page-number (aref next-free-page-number bin-number))
           (tuple (list key-value (page-number record-id))))
      (write-debug "Adding tuple to ~a: ~S (~a)~%" file tuple type-descriptor)
      (write-debug "Hashing key (~S) to bin ~a~%" key-value bin-number)
      (write-debug "Jumping to page with free space (~a)~%" page-number)
      (list (loop
               for page = (read-page file page-number)
               for dirty = (add-tuple-to-page page tuple type-descriptor)
               do (unless dirty
                    (setf (aref next-free-page-number bin-number)
                          (setf page-number (if (zerop (next-page-number page))
                                                (setf (next-page-number page) page-count)
                                                (next-page-number page)))))
               when dirty return dirty)))))

(defmethod delete-tuple-from-index ((file index-file) tuple record-id)
  (with-slots (page-count type-descriptor bin-count
                          next-free-page-number key-field) file
    (write-debug "Deleting tuple from ~a: ~S (~a)~%" file tuple type-descriptor)
    (let* ((key-value (nth key-field tuple))
           (page-number (mod (sxhash key-value) bin-count))
           (tuple (list key-value (page-number record-id)))
           dirtied)
      (loop
         for page = (read-page file page-number)
         for dirty = (delete-tuple-from-page page tuple type-descriptor)
         do (when dirty (push dirty dirtied))
         until (zerop (setf page-number (next-page-number page)))) dirtied)))

(defmethod filter ((file index-file) filter callback)
  ;; @todo IMPORTANT: assumes key is unique, and hence there is only
  ;; one page to fetch from source-file. need to address this issue.
  (with-slots (source-file bin-count type-descriptor key-field) file
    ;; check if the filter is = and the field is our key-field, if so
    ;; we can use the index to filter
    (if (and filter (eq (op filter) '=) (eq (field filter) key-field))
        ;; use (value filter) to find correct page(s) in the index
        (let ((page-number (mod (sxhash (value filter)) bin-count)))
          (loop for page = (read-page file page-number)
             do
               ;; search page for key, and if found, calls filter-page
               ;; on the page the tuple can be found
               (filter-page page type-descriptor (make-instance 'filter :op '= 
                                                                :value (value filter))
                            #'(lambda (tuple type-descriptor record-id)
                                (filter-page (read-page source-file (nth 1 tuple))
                                             (type-descriptor source-file) filter callback)))
             until (zerop (setf page-number (next-page-number page)))))
        ;; defer to the source-file to see if it can do any better
        (filter source-file filter callback))))

  
;;** catalog
  
;; table-info keeps track of the table name and the column names, 
;; which are stored in the catalog
;; note: not currently used yet
(defclass table-info ()
  ((table-name :initarg :table-name :accessor table-name)
   (column-names :initarg :column-names :accessor column-names)))

;; catalog stores the information about tables via a mapping to a table-number
(defclass catalog ()
  ((table-file :initform (make-hash-table) :accessor table-file)
   (table-indexes :initform (make-hash-table) :accessor table-indexes)
   (table-info :initform (make-hash-table) :accessor table-info)))

(defun clear-catalog ()
  (setf *catalog* (make-instance 'catalog)))

;; info methods

;; @todo add methods for setting and getting the info (names) of a table

;; file methods

(defgeneric catalog-add-file (table-number file)
  (:documentation "Addes a file to the catalog and maps it to a table."))

(defun catalog-lookup-file (table-number)
  (gethash table-number (table-file *catalog*)))

(defmethod catalog-add-file (table-number (file file))
  (setf (gethash table-number (table-file *catalog*)) file))

;; indexing methods

(defgeneric catalog-add-index (table-number index)
  (:documentation "Adds an index to the catalog and maps it to a table."))

;; @todo return a list of available indexes
(defun catalog-lookup-index (table-number)
  (gethash table-number (table-indexes *catalog*)))

;; @todo allow more than one index on a table and key-field
(defmethod catalog-add-index (table-number (index index))
  (setf (gethash table-number (table-indexes *catalog*)) index))

;;** bufferpool

(defclass bufferpool ()
  ((pool :initform (make-hash-table) :reader pool)
   (front :initform nil :accessor front)
   (back :initform nil :accessor back)
   (queue-length :initform 0 :accessor queue-length)))

(defun clear-bufferpool ()
  (loop while (front *bufferpool*) do 
       (evict-page-from-bufferpool))
  (setq *bufferpool* (make-instance 'bufferpool)))

(defun fetch-from-bufferpool (id)
    (gethash id (pool *bufferpool*)))

(defun add-to-bufferpool (id page)
  (write-debug "Attempting to add page (~a) to the bufferpool~%" id)
  ;; if full, evict a page
  (with-slots (queue-length pool front back) *bufferpool*
    (when (= queue-length *bufferpool-max-size*)
      (evict-page-from-bufferpool))
    ;; add page
    (setf (gethash id pool) page)
    (let ((id-node (cons id nil)))
      (if (zerop queue-length)
          (setf front id-node)
          (setf (cdr back) id-node))
      (setf back id-node))
    (incf queue-length)))

(defun evict-page-from-bufferpool ()
  (with-slots (queue-length pool front back) *bufferpool*
    (let* ((evicted-id (pop front))
           (evicted-page (gethash evicted-id pool)))
      (write-debug "Evicting page (~a) from the bufferpool~%" evicted-id)
      ;; write evicted-page to disk and remove entry
      (with-slots (record-id dirty?) evicted-page
        (when dirty?
          (write-page (catalog-lookup-file (table-number record-id))
                      evicted-page)))
      (remhash evicted-id pool)
      (when (not front)
        (setf back nil))
      (decf queue-length))))

;;** some utility methods

(defun parse-csv-file (file-name type-descriptor)
  (write-debug "Parsing CSV file.~%")
  (with-open-file (f file-name)
    (loop for line = (read-line f nil)
       while line
       collecting (mapcar 
                   #'(lambda (string-data type)
                       (case type
                         (int (parse-integer string-data))
                         (string string-data)
                         (otherwise nil)))
                   (cl-ppcre:split "," line)
                   type-descriptor))))

(defun heap-file-encoder (input-file-name output-file-name type-descriptor)
  (write-debug "Encoding heap file.~%")
  (let ((heap-file
         (make-instance 'heap-file
                        :file-name output-file-name
                        :type-descriptor type-descriptor 
                        :new? t)))
    (loop for tuple in (parse-csv-file input-file-name type-descriptor)
         do (add-tuple heap-file tuple))
    (write-debug "Clearing the bufferpool.~%")
    (clear-bufferpool) ;; flushes all pages to disk
    heap-file))

;;** parameters

(defparameter *max-int-size* 4294967296
  "The maximum storable INT")

(defparameter *int-size* (ceiling (/ (ceiling (log *max-int-size* 2)) 8))
  "The number of bytes needed for storing an int")

(defparameter *max-table-size* 2147483648
  "The max allowable size of a table (file) on disk")

(defparameter *page-size* 16 ;; 65536
  "The size in bytes of a page.")

(defparameter *index-bin-count* 10
  "Number of bins for static hashing in index.")

(defparameter *max-pages* (floor (/ *max-table-size* *page-size*))
  "The maximum allowable number of pages, computed from *max-table-size*")

(defparameter *page-offset-size* (ceiling (/ (ceiling (log *max-pages* 2)) 8))
  "The size in bytes of the offset storing a page number.")

(defparameter *offset-size* (ceiling (/ (ceiling (log *page-size* 2)) 8))
  "The size in bytes of the offsets within a page.")

(defparameter *bufferpool-max-size* 50
  "Number of pages in the bufferpool")

(defvar *bufferpool* (make-instance 'bufferpool)
  "Buffer of pages")

(defvar *catalog* (make-instance 'catalog)
  "Catalog of information about the database")

;;** example use case

;; used to convert a string tuple to any combination of int or string tuples
;; as long as the string-tuple is all strings of integers
(defun convert-tuple (string-tuple type-descriptor)
  (mapcar #'(lambda (type data) 
              (case type 
                (int (parse-integer data))
                (string data)
                (otherwise nil)))
          type-descriptor string-tuple))
 
(defun main () 
  ;;** command-line arguments
  
  (when (not (= 3 (length *posix-argv*)))
    (format t
"
usage: simpledb <input-file-name> <type-descriptor>

<input-file-name> : Prefix of path to .txt file of data (CSV)
<type-descriptor> : Comma seperated list of column types

example: 

simpledb test int,string

")
    (quit))
  
  (let ((input-file-name (concatenate 'string (nth 1 *posix-argv*) ".txt"))
        (output-file-name (concatenate 'string (nth 1 *posix-argv*) ".dat"))
        (index-file-name (concatenate 'string (nth 1 *posix-argv*) ".idx"))
        (type-descriptor (mapcar 'read-from-string (cl-ppcre:split "," (nth 2 *posix-argv*)))))
    (write-debug "<input-file-name> : ~a~%<output-file-name> : ~a~%<type-descriptor> : ~a~%" 
            input-file-name output-file-name type-descriptor)
    (write-debug "*offset-size* : ~a~%*page-size* : ~a~%*bufferpool-max-size* : ~a~%"
            *offset-size* *page-size* *bufferpool-max-size*)
    (heap-file-encoder input-file-name output-file-name type-descriptor)
    (write-debug "Reading heap file from disk.~%")
    (let* ((heap-file (make-instance 'heap-file
                                     :file-name output-file-name 
                                     :type-descriptor type-descriptor))
           (index-file (make-instance 'index-file
                                      :file-name index-file-name
                                      :source-file heap-file
                                      :key-field 0
                                      :new? t)))
      (clear-bufferpool)

      (write-debug "Printing heap file.~%")
      (print-file heap-file)
      (write-debug "Printing index file.~%")
      (print-file index-file)
      (let ((value (first (convert-tuple '("7") (list (first type-descriptor))))))
        (filter index-file (make-instance 'filter :op '= :field 0 :value value) #'print-tuple)
        (filter index-file (make-instance 'filter :op '<= :field 0 :value value) #'print-tuple))
      (write-debug "Deleting tuples.~%")
      (delete-tuple heap-file (convert-tuple '("1" "2") type-descriptor))
      (delete-tuple heap-file (convert-tuple '("3" "4") type-descriptor))
      (delete-tuple heap-file (convert-tuple '("5" "6") type-descriptor))
      (write-debug "Adding tuples.~%")
      (let ((tuple (convert-tuple '("200" "200") type-descriptor)))
        (loop repeat 10 do (add-tuple heap-file tuple)))
      (write-debug "Dumping back to disk.~%")
      (clear-bufferpool)
      (let ((value (first (convert-tuple '("15") (list (first type-descriptor))))))
        (write-debug "Performing index search.~%")
        (filter index-file (make-instance 'filter :op '= :field 0 :value value) #'print-tuple)
        (write-debug "Performing scan using index.~%")
        (filter index-file (make-instance 'filter :op '< :field 0 :value value) #'print-tuple))
      (write-debug "Deleting all added tuples.")
      (let ((tuple (convert-tuple '("200" "200") type-descriptor)))
        (delete-tuple heap-file tuple))
      (clear-bufferpool)
      (write-debug "Reading heap file from disk.~%")
      (let ((heap-file (make-instance 'heap-file
                                      :file-name output-file-name 
                                      :type-descriptor type-descriptor)))
        (print-file heap-file)
        (write-debug "Reading index file from disk.~%")
        (print-file (make-instance 'index-file
                                   :file-name index-file-name
                                   :source-file heap-file
                                   :key-field 0))
        (clear-bufferpool))))

  (quit))

;;** create binary

(sb-ext:save-lisp-and-die "simpledb" :executable t :toplevel 'main)
