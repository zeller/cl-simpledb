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
;; - Implement the catalog (will be needed by the bufferpool to lookup files by table-number)
;; - Add bufferpool logic and use *bufferpool-max-size*
;; - In delete-tuple-from-page, adjust free-space accordingly
;; - Handle searching for free-space better, currently it iterates over full pages
;; - Need to only write to disk when a page is marked dirty (delete or add occured)
;; - Add conditions/errors on bad input
;; - Package code

;;* Ideas

;; - Can I remove sxhash and just hash the file object itself? Wouldn't that make more sense
;;   record-id would then contain a pointer to the heap-file itself and no need for a catalog
;;   (except for looking up the names and column names of tables)

;;* Code

(require 'cl-ppcre)
(require 'flexi-streams)

;;** some helper functions

;; read-null-terminated-ascii and write-null-terminated-ascii from "Practical Common Lisp"

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

;;** tuple

(defun write-tuple (stream tuple type-descriptor)
  (mapcar #'(lambda (type)
              (case type
                (int (write-byte (pop tuple) stream))
                (string (write-null-terminated-ascii (pop tuple) stream))
                (otherwise nil)))
          type-descriptor))

(defun read-tuple (stream type-descriptor)
  (mapcar #'(lambda (type)
              (case type
                (int (read-byte stream nil))
                (string (read-null-terminated-ascii stream))
                (otherwise nil)))
          type-descriptor))

;;** heap-file

;; heap-file is a list of heap-pages for a single table

(defclass file ()
  ((file-name :initarg :file-name :accessor file-name)
   (type-descriptor :initarg :type-descriptor :accessor type-descriptor)
   (page-count :reader page-count)))

(defmethod initialize-instance :after ((f file) &key new?)
  (when (and new? (probe-file (file-name f)))
    (delete-file (file-name f)))
  (with-open-file (in (file-name f) 
                      :if-exists :supersede
                      :if-does-not-exist :create 
                      :element-type '(unsigned-byte 8))
    (let ((page-count (/ (file-length in) *page-size*)))
      (setf (slot-value f 'page-count) page-count)
      (format t "Found a heap-file with ~a pages.~%" page-count)))
  (catalog-add-file (sxhash (file-name f)) f))

(defgeneric add-tuple (file tuple)
  (:documentation "Adds a tuple to an file. Returns dirtied pages."))

(defgeneric delete-tuple (file tuple)
  (:documentation "Deletes a tuple from a file. Returns dirtied pages."))

(defgeneric print-file (file)
  (:documentation "Prints the contents of a file to *standard-output*"))

(defgeneric write-file (file)
  (:documentation "Flushes a file to disk. Needed for testing."))
                     
(defclass heap-file (file) ())

;; adds a tuple to a file
;; note that read-page will create a new page when an invalid page-number is provided
(defmethod add-tuple ((file heap-file) tuple)
  (format t "Adding tuple: ~a~%" tuple)
  (format t "page-count = ~a~%" (page-count file))
  (let (dirtied)
    (with-slots (page-count type-descriptor) file
      (loop for page-number from 0 to page-count do 
           (let ((page (add-tuple-to-page (read-page file page-number) tuple type-descriptor)))
             (if page (push page dirtied)))
         until dirtied))
    dirtied))

(defmethod delete-tuple ((file heap-file) tuple)
  (let (dirtied)
    (with-slots (page-count type-descriptor) file
      (loop for page-number from 0 to (- page-count 1)do 
           (let ((page (delete-tuple-from-page (read-page file page-number) tuple type-descriptor)))
             (if page (push page dirtied)))))
    dirtied))

(defmethod print-file ((file heap-file))
  (with-slots (type-descriptor page-count) file
    (loop for page-number from 0 to (- page-count 1) do 
         (let ((page (read-page file page-number)))
           (with-slots (tuples) page
             (mapcar #'(lambda (tuple)
                         (format t "~{~a~^, ~}~%" 
                                 (mapcar #'(lambda (data type)
                                             (case type
                                               (int (format nil "~a" data))
                                               (string (format nil "\"~a\"" data))
                                               (otherwise nil)))
                                         tuple type-descriptor))) tuples))))))

(defmethod write-file ((file heap-file))
  (format t "Writing file: ~a (~a)~%" file (file-name file))
  (format t "Writing pages: ~a~%" (page-count file))
  (loop for page-number from 0 to (- (page-count file) 1) do 
       (write-page file (read-page file page-number)))
  (format t "Written.~%"))
  
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
  ((record-id :initarg :record-id :accessor record-id)
   (dirty? :initform nil :accessor dirty?)))

(defgeneric add-tuple-to-page (page tuple type-descriptor)
  (:documentation "Adds a tuple to a page. Returns page or nil if full."))

(defgeneric delete-tuple-from-page (page tuple type-descriptor)
  (:documentation "Deletes a tuple from a page. Returns page or nil if not found."))

(defgeneric write-page (file page)
  (:documentation "Outputs a representation of a page to disk of *page-size* bytes"))

(defgeneric read-page (file page-number)
  (:documentation "Reads a page from disk into a page datastructure"))

;; checks bufferpool for page before fetching page from file
(defmethod read-page :around ((file file) page-number)
  (let ((hash (+ (sxhash (file-name file)) page-number)))
    ;; lookup hash in bufferpool
    (let ((page (fetch-from-bufferpool hash)))
      (if page page ;; page is in bufferpool
          (let ((page (call-next-method)))
            ;; add page to bufferpool, but may need to evict
            (add-to-bufferpool hash page)
            page)))))

(defclass heap-page (page)
  ((tuples :initarg :tuples :accessor tuples)
   (free-space :initarg :free-space :accessor free-space)))

;; returns the size (in bytes) of a tuple on disk
(defun tuple-size (tuple type-descriptor)
  (length (flexi-streams:with-output-to-sequence (stream :element-type '(unsigned-byte 8))
            (write-tuple stream tuple type-descriptor))))

(defmethod add-tuple-to-page ((page heap-page) tuple type-descriptor)
  (format t "Adding tuple to page: ~a~%" tuple)
  (with-slots (tuples free-space) page
    (let ((required-space (tuple-size tuple type-descriptor)))
      (format t "Required space: ~a~%" required-space)
      (if (< free-space required-space) 
          (progn 
            (format t "Not enough free space! (~a < ~a)~%" free-space required-space)
            nil)
          (progn 
            (push tuple tuples)
            (format t "Free space (before): ~a~%" free-space)
            (format t "Free space (after): ~a~%" (setf free-space (- free-space required-space)))
            page)))))

(defmethod delete-tuple-from-page ((page heap-page) tuple type-descriptor)
  (with-slots (tuples record-id free-space) page
    (let ((occurances (count tuple tuples :test 'equal)))
      (when (not (zerop occurances))
        (format t "Deleting ~a occurances of ~a from page ~a.~%" 
                occurances tuple (page-number record-id))
        (setf tuples (delete tuple tuples :test 'equal))
        (incf free-space (* (tuple-size tuple type-descriptor) occurances)))))
  page)
    
;; writes a heap-page to disk
;; assumes that the tuples fit on the page, need to check this when adding tuples!
(defmethod write-page ((file heap-file) (page heap-page))
  (format t "Writing page~%")
  (format t "Tuples: ~a~%" (tuples page))
  (with-slots (file-name type-descriptor) file
    (with-slots (tuples record-id) page
      (with-open-file (f file-name
                         :direction :output 
                         :if-exists :overwrite
                         :if-does-not-exist :create 
                         :element-type '(unsigned-byte 8))
        (let ((start (+ (* (page-number record-id) *page-size*) 1))
              (count 0))
          (file-position f start)
          ;; write-out the tuples to disk using the type to encode the field
          (format t "Writing to location: ~a~%" start)
          (mapcar #'(lambda (tuple)
                      (format t "Writing tuple: ~a (~a)~%" tuple type-descriptor)
                      (write-tuple f tuple type-descriptor)
                      (incf count)) tuples)
          ;; pad with zeros, although not really necessary, looks good in hexdump
          (loop repeat (- (+ start *page-size*) (file-position f) 1) do (write-byte 0 f))
          ;; write-out the number of tuples
          (file-position f (- start 1))
          (write-byte count f))))))

;; reads a heap-page, given by its page-number, from a heap-file
(defmethod read-page ((file heap-file) page-number)
  (let ((heap-page (make-instance 'heap-page)))
    (with-slots (tuples record-id free-space) heap-page
      (with-slots (file-name type-descriptor page-count) file
        (with-open-file (f file-name
                           :element-type '(unsigned-byte 8))
          (setf record-id (make-instance 'record-id 
                                         :table-number (sxhash file-name)
                                         :page-number page-number))
          (cond 
            ((< page-number page-count) ;; page-number is valid
             (format t "Reading page (~a) from file~%" page-number)
             (let ((start (* (page-number record-id) *page-size*)))
               (file-position f start)
               (setf tuples (loop repeat (read-byte f nil) collect (read-tuple f type-descriptor)))
               (setf free-space (- (+ start *page-size*) (file-position f)))))
            ((= page-number page-count) ;; page-number is invalid, but create a new page sequentially
             (format t "Creating a new page (~a)~%" page-number)
             (setf tuples nil)
             (setf free-space (- *page-size* 1))
             (incf page-count))
            (t nil))))) ;; @todo throw an error here, this should never happen
    heap-page))

;;** catalog
  
;; table-info keeps track of the table name and the column names, which are stored in the catalog
(defclass table-info ()
  ((table-name :initarg :table-name :accessor table-name)
   (column-names :initarg :column-names :accessor column-names)))

;; catalog stores the information about tables via a mapping to a table-number
(defclass catalog ()
  ((table-files :initform (make-hash-table) :accessor table-files)
   (table-names :initform (make-hash-table) :accessor table-names)))

(defun clear-catalog ()
  (setf *catalog* (make-instance 'catalog)))

(defun catalog-lookup-file (table-number)
  (gethash table-number (table-files *catalog*)))

(defun catalog-add-file (table-number file)
  (setf (gethash table-number (table-files *catalog*)) file))

;;** bufferpool

(defclass bufferpool ()
  ((pool :initform (make-hash-table) :reader pool)
   (front :initform nil :accessor front)
   (back :initform nil :accessor back)
   (queue-length :initform 0 :accessor queue-length)))

(defun clear-bufferpool ()
    (setq *bufferpool* (make-instance 'bufferpool)))

(defun fetch-from-bufferpool (id)
    (gethash id (pool *bufferpool*)))

(defun add-to-bufferpool (id page)
  (format t "Attempting to add page (~a) to the bufferpool~%" id)
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
      (format t "Evicting page (~a) from the bufferpool~%" evicted-id)
      ;; write evicted-page to disk and remove entry
      (with-slots (record-id) evicted-page
        (write-page (catalog-lookup-file (table-number record-id))
                    evicted-page))
      (remhash evicted-id pool)
      (when (not front)
        (setf back nil))
      (decf queue-length))))

;;** command-line arguments

(when (not (= 3 (length *posix-argv*)))
  (format t 
"
usage: convert <input-file-name> <type-descriptor>

<input-file-name> : Prefix of path to .txt file of data (CSV)
<type-descriptor> : Comma seperated list of column types

example: 

convert test int,string

")
  (quit))

;;** some utility methods

(defun parse-csv-file (file-name type-descriptor)
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
  (let ((heap-file 
         (make-instance 'heap-file
                        :file-name output-file-name 
                        :type-descriptor type-descriptor 
                        :new? t)))
    (loop for tuple in (parse-csv-file input-file-name type-descriptor)
         do (progn (format t "Adding tuple: ~a~%" tuple) (add-tuple heap-file tuple)))
    (write-file heap-file)
    heap-file))

;;** parameters

(defparameter *page-size* 16
  "The size in bytes of a page.")

(defparameter *offset-size* (ceiling (/ (ceiling (log *page-size* 2)) 8))
  "The size in bytes of the offsets within a page.")

(defparameter *bufferpool-max-size* 5
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
  
(let ((input-file-name (concatenate 'string (nth 1 *posix-argv*) ".txt"))
      (output-file-name (concatenate 'string (nth 1 *posix-argv*) ".dat"))
      (type-descriptor (mapcar 'read-from-string (cl-ppcre:split "," (nth 2 *posix-argv*)))))
  (format t "<input-file-name> : ~a~%<output-file-name> : ~a~%<type-descriptor> : ~a~%" 
          input-file-name output-file-name type-descriptor)
  (format t "Encoding heap file.~%")
  (heap-file-encoder input-file-name output-file-name type-descriptor)
  (clear-bufferpool)
  (format t "Reading heap file from disk.~%")
  (let ((heap-file (make-instance 'heap-file
                                  :file-name output-file-name 
                                  :type-descriptor type-descriptor)))
    (print-file heap-file)
    (format t "Deleting tuples.~%")
    (delete-tuple heap-file (convert-tuple '("1" "2") type-descriptor))
    (delete-tuple heap-file (convert-tuple '("3" "4") type-descriptor))
    (delete-tuple heap-file (convert-tuple '("5" "6") type-descriptor))
    (format t "Adding tuples.~%")
    (loop repeat 10 do (add-tuple heap-file (convert-tuple '("200" "200") type-descriptor)))
    (format t "Dumping back to disk.~%")
    (write-file heap-file)
    (clear-bufferpool)
    (format t "Reading heap file from disk.~%")
    (print-file (make-instance 'heap-file
                               :file-name output-file-name 
                               :type-descriptor type-descriptor))))

;;** exit sbcl

(quit)