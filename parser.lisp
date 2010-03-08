(in-package :simpledb)

#+asdf(eval-when (:compile-toplevel :execute :load-toplevel)
        (asdf:oos 'asdf:load-op :fucc-parser))
#+asdf(eval-when (:compile-toplevel :execute)
        (asdf:oos 'asdf:load-op :fucc-generator))

(fucc:defparser *query-parser* s
  (= < > <= >= :semicolon :id :const :asterisk
     :comma :period
     select delete from where
     or and :lparen :rparen)
  ((s ->
      (:var query-list (:list query :semicolon))
      (:maybe :semicolon)
      (:do query-list))
   
   (query -> 
          (:or select-query
               delete-query))
   
   (select-query ->
                 select (:var fields (:or :asterisk fields))
                 from (:var tables tables)
                 (:var where (:maybe where-statement))
                 (:do `(select ,fields ,tables ,@where)))

   (delete-query -> 
                 delete from (:var table table) 
                 (:var where where-statement)
                 (:do `(delete ,table ,@where)))
   
   (where-statement ->
                    where
                    (:var clause clause)
                    (:do (list 'where clause)))
   
   (clause ->
           :lparen (:var clause clause) :rparen
           (:do clause)
           ->
           clause0 (:or or and) clause
           (:call (lambda (a op b) (list op a b)))
           ->
           clause0)

   (clause0 ->
            field (:or = < > <= >=) value
            (:call (lambda (field op value) (list op field value))))

   (value ->
          field
          ->
          :const)

   (tables ->
           (:var table :id) :comma (:var tables tables)
           (:do (push table tables))
           ->
           (:var table table)
           (:do (list table)))

   (table ->
          (:var table :id)
          (:do table))

   (fields ->
           (:var field field) :comma (:var fields fields)
           (:do (push field fields))
           ->
           (:var field field)
           (:do (list field)))

   (field ->
          table :period (:or :id :asterisk)
          (:call (lambda (table op field) (cons table field)))
          ->
          :id))
  :prec-info
  ((:right or and)))

(fucc:defparser *schema-parser* s
  (:id
   string int
   :comma :semicolon
   :lparen :rparen)
  ((s ->
      (:var table-list (:list table :semicolon))
      (:maybe :semicolon)
      (:do table-list))
   
   (table ->
          (:var table :id)
          :lparen
          (:var fields fields)
          :rparen
          (:do (list table fields)))

   (fields ->
           (:var field field) :comma (:var fields fields)
           (:do (push field fields))
           ->
           (:var field field)
           (:do (list field)))

   (field ->
          :id (:or int string)
          (:call (lambda (name type) (cons name type))))))

(defun simpledb-lexer (list)
  "Return lexical analizer for list of tokens"
  (lambda ()
    (let ((next-value (pop list)))
      (cond
        ((null next-value)
         (values nil nil))
        ((member next-value '(:semicolon #\;))
         (values :semicolon :semicolon))
        ((member next-value '(:comma #\,))
         (values :comma :comma))
        ((member next-value '(:lparen #\())
         (values :lparen :lparen))
        ((member next-value '(:rparen #\)))
         (values :rparen :rparen))
        ((member next-value '(:period #\.))
         (values :period :period))
        ((member next-value '(:asterisk #\*))
         (values :asterisk :asterisk))
        ((member next-value '(select delete from where = < > <= >= or and int string))
         (values next-value next-value))
        ((symbolp next-value)
         (values :id next-value))
        ((numberp next-value)
         (values :const next-value))
        ((stringp next-value)
         (values :const next-value))
        (t
         (error "Unknown token: ~S" next-value))))))

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
              (write-debug "Where: ~A~%" where-clause))
            (error 'unsupported-feature-error :name "DELETE"))
           ('select 
            (write-debug "SELECT STATEMENT~%")
            (let ((fields (second query))
                  (tables (mapcar
                           #'(lambda (table) 
                               (cons (string table)
                                     (catalog-lookup-table-number table)))
                           (third query)))
                  (where-clause (fifth query)))
              (write-debug "Fields: ~S~%" fields)
              (write-debug "Tables: ~S~%" tables)
              (write-debug "Where: ~S~%" where-clause)
              (when where-clause
                (error 'unsupported-feature-error :name "WHERE"))
              (let ((cursor (projection (join tables) fields)))
                (format t "Header: ~S~%" (cursor-info cursor))
                (cursor-open cursor)
                (loop until (cursor-finished-p cursor)
                   for tuple = (cursor-next cursor)
                   do (print-tuple tuple (cursor-type-descriptor cursor)))))))
       while abstract-syntax-tree)))

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
             (let* ((table-name (first table))
                    (fields (second table))
                    (column-names (mapcar #'car fields))
                    (type-descriptor (mapcar #'cdr fields))
                    (heap-file-name (concatenate 'string schema-dir-name (string table-name) ".dat"))
                    (index-file-name (concatenate 'string schema-dir-name (string table-name) ".idx")))
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
