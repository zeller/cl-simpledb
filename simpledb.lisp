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
 
(defun parse-type-descriptor (type-descriptor-string)
  (map 'list
       #'(lambda (type)
           (cond
             ((string= "int" type) 'int)
             ((string= "string" type) 'string)
             (t nil)))
       (cl-ppcre:split "," type-descriptor-string)))

;; read standard-input into a list of symbols
;; pass back the symbols or nil if quitting
(defun lexer (&optional (stream *standard-input*))
  (loop
     (let ((c (read-char stream nil nil)))
       (cond
         ((member c '(#\Space #\Tab)))
         ((member c '(nil #\Newline)) (return-from lexer c))
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
          nil)))))

(defun reorder (list-of-items order)
  (mapcar #'(lambda (pos) (nth pos list-of-items)) order))

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
                  (tables (mapcar
                           #'(lambda (table) 
                               (cons (string table)
                                     (catalog-lookup-table-number (string table))))
                           (third query)))
                  (where-clause (fifth query)))
              (write-debug "Fields: ~S~%" fields)
              (write-debug "Tables: ~S~%" tables)
              (write-debug "Where: ~S~%" where-clause)
              (loop for (table-name . table-number) in tables
                 do (let* ((file (catalog-lookup-file table-number))
                           (info (catalog-lookup-info table-number))
                           (column-names (column-names info)))
                      (write-debug "Table info: ~S~%" (column-names info))
                      (filter file nil
                              #'(lambda (tuple type-descriptor record-id)
                                  ;; print only the valid rows and columns
                                  (if (eq :asterisk fields)
                                      (print-tuple tuple type-descriptor nil)
                                      (let ((order (mapcar
                                                    #'(lambda (field)
                                                        (position field column-names :test #'string=)) fields)))
                                        (print-tuple (reorder tuple order) (reorder type-descriptor order) nil))))))))))
       while abstract-syntax-tree)))

(defun maybe-unread (char stream)
  (when char
    (unread-char char stream)))

(defun intern-id (string)
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

(defun lex (&optional (newlines? nil))
  (let ((e '()))
    (loop 
       (let ((symbol (lexer)))
         (when (or (null symbol)
                   (and (not newlines?) (eq #\Newline symbol)))
           (return-from lex (nreverse e)))
         (when (not (eq #\Newline symbol))
           (push symbol e))))))

(defun parse-schema (schema-file-name)
  (with-open-file (*standard-input* schema-file-name)
    (let ((schema-dir-name (subseq schema-file-name 
                                   0 (+ (position #\/ schema-file-name :from-end t) 1)))
          (expression (lex t)))
      (write-debug "~A~%" expression)
      (let ((abstract-syntax-tree
             (fucc:parser-lr (simpledb-lexer expression) *schema-parser*)))
        (format t "Loaded schema: ~A~%" abstract-syntax-tree)
        (loop for table = (pop abstract-syntax-tree)
           do 
             (write-debug "~A~%" table)
             (let* ((table-name (string (first table)))
                    (fields (second table))
                    (column-names (mapcar #'string (mapcar #'car fields)))
                    (type-descriptor (mapcar #'cdr fields))
                    (heap-file-name (concatenate 'string schema-dir-name table-name ".dat"))
                    (index-file-name (concatenate 'string schema-dir-name table-name ".idx")))
               (write-debug "Loading table ~a~%" table-name)
               (write-debug "Reading heap file from disk (~a).~%" heap-file-name)
               (write-debug "Reading index file from disk (~a).~%" index-file-name)
               (let* ((heap-file (make-instance 'heap-file
                                                :file-name heap-file-name
                                                :type-descriptor type-descriptor))
                      (index-file (make-instance 'index-file
                                                 :file-name index-file-name
                                                 :source-file heap-file
                                                 :key-field 0))
                      (table-info (make-instance 'table-info
                                                 :table-name table-name
                                                 :column-names column-names)))
                 (write-debug "Adding the info ~a to the catalog~%" table-info)
                 (catalog-add-info (sxhash heap-file-name) table-info)))
           while abstract-syntax-tree)))))

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
         (table-does-not-exist-error (e)
           (format t "error: table (~a) does not exist~%" (name e)))))))

(defun change-file-type (input-file-name file-type)
  (let ((input-dir-name-end (position #\/ input-file-name :from-end t))
        (input-file-name-end (position #\. input-file-name :from-end t :test #'char=)))
    (when input-dir-name-end
      (let ((input-dir (subseq input-file-name 0 input-dir-name-end))
            (input-file (string-upcase (subseq input-file-name (+ input-dir-name-end 1)))))
        (setf input-file-name (format nil "~a/~a" input-dir input-file))))
    (concatenate 'string (subseq input-file-name 0 input-file-name-end) file-type)))

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
