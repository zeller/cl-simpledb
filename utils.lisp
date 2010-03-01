(in-package :simpledb)

;;** some helper functions

(defmacro write-debug (&rest args)
  (if nil ;; set to t or nil to toggle debugging information output
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
