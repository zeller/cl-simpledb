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

;; - Implement Filter as an 'interator' rather than a callback.

;;* Ideas

;; - Can I remove sxhash and just hash the file object itself?
;;   Wouldn't that make more sense record-id would then contain a
;;   pointer to the heap-file itself and no need for a catalog (except
;;   for looking up the names and column names of tables)

;;* Code

(in-package :simpledb)

(export '(simpledb))

;;** example use case

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

;; used to convert a string tuple to any combination of int or string tuples
;; as long as the string-tuple is all strings of integers
(defun convert-tuple (string-tuple type-descriptor)
  (mapcar #'(lambda (type data) 
              (case type 
                ('int (parse-integer data))
                ('string data)
                (otherwise nil)))
          type-descriptor string-tuple))

(defun convert (input-file-name output-file-name index-file-name type-descriptor)
  (let (heap-file index-file)
    
    ;; echo parsed command-line arguments
    (write-debug 
     "<input-file-name> : ~a~%<output-file-name> : ~a~%<index-file-name> : ~a~%<type-descriptor> : ~a~%"
     input-file-name output-file-name index-file-name type-descriptor)
    
    ;; create a heap-file on disk from <input-file-name> in <output-file-name>
    (heap-file-encoder input-file-name output-file-name type-descriptor)
    
    ;; reread the heap-file from disk, create index-file from heap-file
    (write-debug "Reading heap file from disk.~%")
    
    (setf heap-file (make-instance 'heap-file
                                   :file-name output-file-name 
                                   :type-descriptor type-descriptor))
    
    ;; print the contents of the files 
    ;; note: (index-file never touches it's own pages)
    (write-debug "Printing heap file.~%")
    (write-debug "~a ~a ~a ~a~%" *max-int-size* *int-size* *page-offset-size* *offset-size*)
    (print-file heap-file)
    
    (setf index-file (make-instance 'index-file
                                    :file-name index-file-name
                                    :source-file heap-file
                                    :key-field 0
                                    :new? t))
    
    ;; force index-file to disk
    (clear-bufferpool)
    
    (write-debug "Printing index file.~%")
    (print-file index-file)
    
    (let ((value (first (convert-tuple '("7") (list (first type-descriptor))))))
      (filter index-file (make-instance 'filter :op '= :field 0 :value value) #'print-tuple)
      (filter index-file (make-instance 'filter :op '<= :field 0 :value value) #'print-tuple))
    
    ;; delete a few tuples, and add a few tuples
    (write-debug "Deleting tuples.~%")
    (delete-tuple heap-file (convert-tuple '("1" "2") type-descriptor))
    (delete-tuple heap-file (convert-tuple '("3" "4") type-descriptor))
    (delete-tuple heap-file (convert-tuple '("5" "6") type-descriptor))
    (write-debug "Adding tuples.~%")
    (let ((tuple (convert-tuple '("200" "200") type-descriptor)))
      (loop repeat 10 do (add-tuple heap-file tuple)))
    
    ;; force back to disk
    (write-debug "Dumping back to disk.~%")
    (clear-bufferpool)
    
    ;; perform an index search to check that we only use the pages in a single bin
    ;; and that we fetch a single page from heap-file
    (let ((value (first (convert-tuple '("15") (list (first type-descriptor))))))
      (write-debug "Performing index search.~%")
      (filter index-file (make-instance 'filter :op '= :field 0 :value value) #'print-tuple)
      (write-debug "Performing scan using index.~%")
      (filter index-file (make-instance 'filter :op '< :field 0 :value value) #'print-tuple))
    
    ;; perform a delete on the heap-file, this should also update any indexes
    (write-debug "Deleting all added tuples.")
    (let ((tuple (convert-tuple '("200" "200") type-descriptor)))
      (delete-tuple heap-file tuple))
    
    ;; force to disk
    (clear-bufferpool)
    
    ;; finally, reread from disk, print contents and flush back to disk
    ;; note: no pages should be written to disk in this process
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
 
(defun parse-type-descriptor (type-descriptor-string)
  (map 'list
       #'(lambda (type)
           (cond
             ((string= "int" type) 'int)
             ((string= "string" type) 'string)
             (t nil)))
       (cl-ppcre:split "," type-descriptor-string)))

(defun change-file-type (input-file-name file-type)
  (concatenate 'string 
               (subseq input-file-name 0
                       (position #\. input-file-name :test #'char=))
               file-type))

;; read standard-input into a list of symbols
;; pass back the symbols or nil if quitting
(defun lexer (&optional (stream *standard-input*))
  (loop
     (let ((c (read-char stream nil nil)))
       (cond
         ((member c '(#\Space #\Tab)))
         ((member c '(nil #\Newline)) (return-from lexer nil))
         ((member c '(#\, #\. #\; #\( #\) #\*))
          (return-from lexer c))
         ((member c '(#\< #\> #\=))
          (unread-char c stream)
          (return-from lexer (read-operator stream)))
         ((digit-char-p c)
          (unread-char c stream)
          (return-from lexer (read-number stream)))
         ((char= c #\")
          (unread-char c stream)
          (return-from lexer (read-string stream)))
         ((alpha-char-p c)
          (unread-char c stream)
          (return-from lexer (read-id stream)))
         (t
          (error c))))))

(defun evaluate (expression)
  (let ((abstract-syntax-tree
         (fucc:parser-lr (simpledb-lexer expression) *query-parser*)))
    (loop for query = (pop abstract-syntax-tree)
       do 
         (write-debug "~A~%" query)
         (case (first query)
           ('delete 
            (write-debug "DELETE STATEMENT~%")
            (let ((table (second query))
                  (where-clause (fourth query)))
              (write-debug "Table: ~A~%" table)
              (write-debug "Where: ~A~%" where-clause)))
           ('select 
            (write-debug "SELECT STATEMENT~%")
            (let ((fields (second query))
                  (tables (third query))
                  (where-clause (fifth query)))
              (write-debug "Fields: ~A~%" fields)
              (write-debug "Tables: ~A~%" tables)
              (write-debug "Where: ~A~%" where-clause))))
       while abstract-syntax-tree)))

(defun maybe-unread (char stream)
  (when char
    (unread-char char stream)))

(defun intern-id (string)
  ;; I'd really like to say (intern (string-upcase string) '#.*package*),
  ;; but that breaks Allegro's case hacks.
  (let ((*package* '#.*package*))
    (read-from-string string)))

(defun read-id (stream)
  (let ((v '()))
    (loop
       (let ((c (read-char stream nil nil)))
         (when (or (null c)
                   (not (or (digit-char-p c) (alpha-char-p c) (eql c #\_))))
           (maybe-unread c stream)
           (when (null v)
             (lexer-error c))
           (return-from read-id (intern-id (coerce (nreverse v) 'string))))
         (push c v)))))

(defun read-string (stream)
  (let ((v '()))
    (loop
       (let ((c (read-char stream nil nil)))
         (if (eql c #\")
             (when (not (null v))
               (return-from read-string (coerce (nreverse v) 'string)))
             (if (null c)
                 (lexer-error c)
                 (push c v)))))))

(defun read-operator (stream)
  (let ((v '()))
    (let ((c (read-char stream nil nil)))
      (cond
        ((member c '(#\< #\>))
         (push c v)
         (let ((c (read-char stream nil nil)))
           (if (char= c #\=) (push c v) (maybe-unread c stream))))
        ((member c '(#\=))
         (push c v))))
    (return-from read-operator (intern-id (coerce (nreverse v) 'string)))))

(defun read-number (stream)
  (let ((v nil))
    (loop
       (let ((c (read-char stream nil nil)))
         (when (or (null c) (not (digit-char-p c)))
           (maybe-unread c stream)
           (when (null v)
             (lexer-error c))
           (return-from read-number v))
         (setf v (+ (* (or v 0) 10) (- (char-code c) (char-code #\0))))))))

(defun lex ()
  (let ((e '()))
    (loop 
       (let ((symbol (lexer)))
         (when (null symbol)
           (return-from lex (nreverse e)))
         (push symbol e)))))

(defun parser (schema-file-name)
  ;; 1. parse schema file and read tables and indexes
  ;; @todo

  ;; 2. REPL for SQL from *standard-input*
  (format t "Type a SQL query, or an empty line to quit.~%")
  (loop
     (with-simple-restart (abort "Return to simpledb toplevel.")
       (format t "> ")
       (finish-output *standard-output*)
       (let ((e (lex)))
         (when (null e)
           (return-from parser))
         (format t " => ~A~%" (evaluate e))))))

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

  
