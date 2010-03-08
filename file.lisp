(in-package :simpledb)

;;** file

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

(defgeneric add-tuple (file tuple)
  (:documentation "Adds a tuple to an file. Returns dirtied pages."))

(defgeneric delete-tuple (file tuple)
  (:documentation "Deletes a tuple from a file. Returns dirtied pages."))

(defgeneric print-file (file)
  (:documentation "Prints the contents of a file to *standard-output*"))

(defun print-tuple (tuple type-descriptor)
  (format t "~{~a~^, ~}~%"
          (mapcar #'(lambda (data type)
                      (case type
                        ('int (format nil "~a" data))
                        ('string (format nil "\"~a\"" data))
                        (otherwise nil)))
                  tuple type-descriptor)))

(defmethod print-file ((file file))
  (write-debug "Printing file.~%")
  (let ((cursor (make-cursor-for file)))
    (loop until (cursor-finished-p cursor)
         for tuple = (cursor-next cursor)
         do (print-tuple tuple (cursor-type-descriptor cursor)))))

;;** page 

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

(defun read-offset (in &optional (byte-count *offset-size*))
  (let ((value 0)
        (offset 0))
    (loop repeat byte-count do
         (progn 
           (setf (ldb (byte 8 offset) value) (read-byte in nil 0))
           (incf offset 8)))
    value))

(defun write-offset (value out &optional (byte-count *offset-size*))
  (let ((offset 0))
    (loop repeat byte-count do 
         (progn
           (write-byte (ldb (byte 8 offset) value) out)
           (incf offset 8)))))

(defun read-int (in)
  (read-offset in *int-size*))

(defun write-int (value out)
  (write-offset value out *int-size*))

;;** tuple

(defun write-tuple (stream tuple type-descriptor)
  (mapcar #'(lambda (type)
              (case type
                ('int (write-int (pop tuple) stream))
                ('string (write-null-terminated-ascii (pop tuple) stream))
                (otherwise nil)))
          type-descriptor))

(defun read-tuple (stream type-descriptor)
  (mapcar #'(lambda (type)
              (case type
                ('int (read-int stream))
                ('string (read-null-terminated-ascii stream))
                (otherwise nil)))
          type-descriptor))

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

(defgeneric get-binary-page (page type-descriptor)
  (:documentation "Get a binary representation of a page that can be written to disk."))

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

;;** heap-file

;; heap-file is a list of heap-pages for a single table
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
              (write-offset next-page-number out *page-offset-size*)
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
                 (setf (slot-value heap-page 'next-page-number) (read-offset in *page-offset-size*))
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
      (let ((cursor (make-cursor-for source-file)))
        (loop until (cursor-finished-p cursor)
           for tuple = (cursor-next cursor)
           ;; @todo need to replace nil with a record-id
           do (add-tuple-to-index file tuple nil))))))

(defmethod add-tuple-to-index ((file index-file) tuple record-id)
  (write-debug "Original tuple: ~S~%" tuple)
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
