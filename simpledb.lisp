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

;; - Support aliases in SELECT

;; - Keep tuples sorted on page (only tiny overhead when
;;   inserting/deleting)

;; - Fix table ids, currently they are not completely unique (based on
;;   filename)

;; - Support WHERE clauses using the filter

;; - Push projections deeper once a query optimizer is implemented.

;; - Need to look at index-files, made some breaking changes. For
;;   instance, a cursor is needed for searching a index instead of the
;;   previous callback method. The initialize-instance for index-file
;;   also requires the record-id of the tuple from the cursor, which
;;   is not currently supported. See cursor.lisp:filter and
;;   file.lisp:index-file

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

(in-package :simpledb)

(export '(simpledb))

(defparameter *usage-string*
"
usage: simpledb convert <input-file-name> <type-descriptor>
       simpledb parser <schema-file-name>

convert
-------

<input-file-name> - Pathname of CSV file containing data,
                    prefix of filename will be used as tablename
<type-descriptor> - Comma seperated list of column types

parser
------

<schema-file-name> - Pathname of file containing information about tables

examples
--------

simpledb convert data/first.txt int,string
simpledb convert data/last.txt string,int
simpledb parser data/schema.txt

")

(defun convert (input-file-name output-file-name index-file-name type-descriptor)
  ;; create a heap-file on disk from <input-file-name> in <output-file-name>
  (heap-file-encoder input-file-name output-file-name type-descriptor)
    
  ;; reread the heap-file from disk, create index-file from heap-file
  (write-debug "Reading heap file from disk.~%")    
  (let ((heap-file (make-instance 'heap-file
                                  :file-name output-file-name 
                                  :type-descriptor type-descriptor)))
    
    ;; print the contents of the file
    (write-debug "Printing heap file.~%")
    (print-file heap-file)
    
    (write-debug "Building index file.~%")
    (make-instance 'index-file
                   :file-name index-file-name
                   :source-file heap-file
                   :key-field 0
                   :new? t)

    ;; force index-file to disk
    (clear-bufferpool)))

(define-condition table-does-not-exist-error (error)
  ((name :initarg :name :reader name)))

(define-condition field-does-not-exist-error (error)
  ((name :initarg :name :reader name)))

(define-condition unsupported-feature-error (error)
  ((name :initarg :name :reader name)))

(define-condition ambiguous-field-error (error)
  ((name :initarg :name :reader name)))
 
(defun parser (schema-file-name)
  ;; 1. parse schema file and read tables and indexes
  (handler-case
      (parse-schema schema-file-name)
    (sb-int:simple-file-error ()
      (format t "abort: schema file does not exist~%")
      (return-from parser))
    (error ()
      (format t "abort: could not load schema~%")
      (return-from parser)))

  ;; 2. REPL for SQL from *standard-input*
  (format t "Type a SQL query, or an empty line to quit.~%")
  (loop
     (with-simple-restart (abort "Return to simpledb toplevel.")
       (format t "> ")
       (finish-output *standard-output*)
       (handler-case 
           (let ((e (lex)))
             (when (null e)
               (return-from parser))
             (evaluate e))
         (fucc:lr-parse-error-condition ()
           (format t "error: SQL malformed~%"))
         (ambiguous-field-error (e)
           (format t "error: ~a is ambiguous.~%" (name e)))
         (unsupported-feature-error (e)
           (format t "error: ~a is currently unsupported~%" (name e)))
         (field-does-not-exist-error (e)
           (format t "error: field (~a) does not exist~%" (name e)))
         (table-does-not-exist-error (e)
           (format t "error: table (~a) does not exist~%" (name e)))))))

(defun simpledb ()
  ;;** command-line arguments
  (let ((tool-name (second sb-ext:*posix-argv*)))
    (cond
      ((string= tool-name "convert")
       (when (not (= 4 (length sb-ext:*posix-argv*)))
         (format t *usage-string*)
         (return-from simpledb 1))
       (let* ((input-file-name (nth 2 sb-ext:*posix-argv*))
              (output-file-name (change-file-type input-file-name ".dat"))
              (index-file-name (change-file-type input-file-name ".idx"))
              (type-descriptor (parse-type-descriptor (nth 3 sb-ext:*posix-argv*))))
         (convert input-file-name output-file-name index-file-name type-descriptor)))
      ((string= tool-name "parser")
       (when (not (= 3 (length sb-ext:*posix-argv*)))
         (format t *usage-string*)
         (return-from simpledb 1))
       (let ((schema-file-name (nth 2 sb-ext:*posix-argv*)))
         (parser schema-file-name)))
      (t
       (format t *usage-string*)
       (return-from simpledb 1)))))
