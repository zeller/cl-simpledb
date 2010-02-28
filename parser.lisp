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
   
   (query -> (:or select-query
                  delete-query))
   
   (select-query ->
                 select (:var fields fields) 
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
           clause (:or or and) clause
           (:call (lambda (a op b) (list op a b)))
           ->
           field (:or = < > <= >=) :const
           (:call (lambda (field op value) (list op field value))))

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
          :id :period (:or :id :asterisk)
          (:call (lambda (table op field) (cons table field)))
          ->
          (:var field :id)
          ->
          :asterisk
          (:do (list :asterisk))))
  :prec-info
  ((:right or and)))

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
        ((member next-value '(select delete from where = < > <= >= or and))
         (values next-value next-value))
        ((symbolp next-value)
         (values :id next-value))
        ((numberp next-value)
         (values :const next-value))
        ((stringp next-value)
         (values :const next-value))
        (t
         (error "Unknown token: ~S" next-value))))))

(defun test-sql (list)
  (fucc:parser-lr
   (simpledb-lexer list)
   *query-parser*))

;; (test-sql (copy-list 
;;            '(select name #\, id #\, info #\. salary from emp #\, info where id <= 2 #\; 
;;              delete from emp where id = 2 and #\( id <= 2 or id >= 0 #\) and id = 1 #\;
;;              select id from emp where emp #\. name = "mike" #\;
;;              select id from emp)))
