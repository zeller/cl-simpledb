(in-package :simpledb)

;;** some helper functions

(defmacro write-debug (&rest args)
  (if t ;; set to t or nil to toggle debugging information output
      `(format t ,@args)))

;;** some utility methods

(defun parse-csv-file (file-name type-descriptor)
  (write-debug "Parsing CSV file.~%")
  (with-open-file (f file-name)
    (loop for line = (read-line f nil)
       while line
       collecting (mapcar 
                   #'(lambda (string-data type)
                       (case type
                         ('int (parse-integer string-data))
                         ('string string-data)
                         (otherwise nil)))
                   (cl-ppcre:split "," line)
                   type-descriptor))))

(defun heap-file-encoder (input-file-name output-file-name type-descriptor)
  (write-debug "Encoding heap file.~%")
  (let ((heap-file
         (make-instance 'heap-file
                        :file-name output-file-name
                        :type-descriptor type-descriptor 
                        :new? t))
        (tuples (parse-csv-file input-file-name type-descriptor)))
    (write-debug "Parsed tuples: ~S~%" tuples)
    (loop for tuple in tuples
         do (add-tuple heap-file tuple))
    (write-debug "Clearing the bufferpool.~%")
    (clear-bufferpool) ;; flushes all pages to disk
    heap-file))

(defun change-file-type (input-file-name file-type)
  (let ((input-dir-name-end (position #\/ input-file-name :from-end t))
        (input-file-name-end (position #\. input-file-name :from-end t :test #'char=)))
    (when input-dir-name-end
      (let ((input-dir (subseq input-file-name 0 input-dir-name-end))
            (input-file (string-upcase (subseq input-file-name (+ input-dir-name-end 1)))))
        (setf input-file-name (format nil "~a/~a" input-dir input-file))))
    (concatenate 'string (subseq input-file-name 0 input-file-name-end) file-type)))

(defun parse-type-descriptor (type-descriptor-string)
  (map 'list
       #'(lambda (type)
           (cond
             ((string= "int" type) 'int)
             ((string= "string" type) 'string)
             (t nil)))
       (cl-ppcre:split "," type-descriptor-string)))


(defun reorder (list-of-items order)
  (if order
    (mapcar #'(lambda (pos) (nth pos list-of-items)) order)
    list-of-items))

(defun flatten (l)
  (cond
    ((null l) nil)
    ((atom l) (list l))
    (t (append (flatten (car l))
               (flatten (cdr l))))))
