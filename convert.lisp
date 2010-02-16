#|
exec sbcl --noinform --load $0 --end-toplevel-options "$@"
|#
;; -*- mode: lisp; mode: outline-minor; -*-

;; Copyright (C) 2009 Michael Zeller

;;* Commentary
;;
;; This file contains code relating to recording screencasts of the current window.
;;

;;* Tasks
;;
;; - Add some OO to this. buffer-pool, heap-page, heap-file

;; heap-page: a *page-size* length sequence of bytes
;; heap-file: a collection of heap-pages. keeps track of 
;; the heap-file-name, and can read and write pages.

;; - Add unit tests.

;;* Code

(require 'cl-ppcre)

;; variables

(defparameter *page-size* 4096)

;; some helper functions

(defun parse-csv-file (file type-descriptor)
  (with-open-file (f file)
    (loop for line = (read-line f nil)
       while line
       collecting (mapcar 
                   #'(lambda (string-data type)
                       (case type
                         (int (parse-integer string-data))
                         (string string-data)
                         (otherwise nil))) ;; @todo throw error in future
                   (cl-ppcre:split "," line)
                   type-descriptor))))

(defun print-heap-file (heap-file type-descriptor)
  (defun print-tuple (tuple)
    (format t "~{~a~^, ~}~%" 
            (mapcar #'(lambda (data type)
                        (case type
                          (int (format nil "~a" data))
                          (string (format nil "\"~a\"" data))
                          (otherwise nil)))
                    tuple type-descriptor)))
  (mapcar 'print-tuple heap-file))

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

;; the primary work horses

(defun write-heap-file (file type-descriptor heap-file)
  (with-open-file (f file 
                     :direction :output 
                     :if-exists :supersede 
                     :if-does-not-exist :create 
                     :element-type '(unsigned-byte 8))
    (defun write-tuple (tuple)
      (mapcar #'(lambda (type)
                  (case type
                    (int (write-byte (pop tuple) f))
                    (string (write-null-terminated-ascii (pop tuple) f))
                    (otherwise nil)))
              type-descriptor)) ;; @todo throw error in future
    (mapcar 'write-tuple heap-file)))

(defun read-heap-file (file type-descriptor)
  (with-open-file (f file
                     :element-type '(unsigned-byte 8))
    (defun read-tuple ()
      (let ((tuple (mapcar 
                    #'(lambda (type)
                        (case type
                          (int (read-byte f nil))
                          (string (read-null-terminated-ascii f))
                          (otherwise nil)))
                    type-descriptor)))
        (if (not (member nil tuple)) tuple)))
    (loop for tuple = (read-tuple)
         while tuple
         collecting tuple)))

(defun heap-file-encoder (in out type-descriptor)
  (let ((heap-file (parse-csv-file in type-descriptor)))
    (write-heap-file out type-descriptor heap-file)))

(defun heap-add-tuple (heap-file tuple)
  (nconc heap-file tuple))

(defun heap-delete-tuple (heap-file tuple)
  (delete tuple heap-file :test 'equal))

;; command-line arguments

(when (not (= 3 (length *posix-argv*)))
  (format t 
"
usage: convert <input-file-name> <type-descriptor>

<input-file-name> : Path to CSV file of data
<type-descriptor> : Comma seperated list of column types

example: 

convert test.txt int,string

")
  (quit))

;; example use case

(let ((input-file-name (concatenate 'string (nth 1 *posix-argv*) ".txt"))
      (output-file-name (concatenate 'string (nth 1 *posix-argv*) ".dat"))
      (type-descriptor (mapcar 'read-from-string (cl-ppcre:split "," (nth 2 *posix-argv*)))))

  (format t 
"<input-file-name> : ~a
<output-file-name> : ~a
<type-descriptor> : ~a
" input-file-name output-file-name type-descriptor)

  (heap-file-encoder input-file-name output-file-name type-descriptor)
  (let ((heap-file (read-heap-file output-file-name type-descriptor)))
    (print-heap-file heap-file type-descriptor)
    (heap-add-tuple heap-file (copy-list (last heap-file)))
    (setf heap-file (heap-delete-tuple heap-file (first heap-file)))
    (write-heap-file output-file-name type-descriptor heap-file)
    (print-heap-file (read-heap-file output-file-name type-descriptor)
                     type-descriptor)))

;; exit sbcl

(quit)