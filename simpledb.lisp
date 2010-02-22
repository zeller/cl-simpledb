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
usage: simpledb <input-file-name> <type-descriptor>

<input-file-name> : Prefix of path to .txt file of data (CSV)
<type-descriptor> : Comma seperated list of column types

example: 

simpledb test int,string

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
 
(defun simpledb ()
  ;;** command-line arguments
  (when (not (= 3 (length sb-ext:*posix-argv*)))
    (format t *usage-string*)
    (return-from simpledb 1))
  
  (let ((input-file-name (concatenate 'string (nth 1 sb-ext:*posix-argv*) ".txt"))
        (output-file-name (concatenate 'string (nth 1 sb-ext:*posix-argv*) ".dat"))
        (index-file-name (concatenate 'string (nth 1 sb-ext:*posix-argv*) ".idx"))
        (type-descriptor (map 'list 
                              #'(lambda (type)
                                 (cond
                                   ((string= "int" type) 'int)
                                   ((string= "string" type) 'string)
                                   (t nil)))
                              (cl-ppcre:split "," (nth 2 sb-ext:*posix-argv*))))
        heap-file index-file)
    
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
 