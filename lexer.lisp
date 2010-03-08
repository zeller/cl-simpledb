(in-package :simpledb)

;; lex an input-stream to symbols for use in the parser

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

(defun lex (&optional (newlines? nil))
  (let ((e '()))
    (loop 
       (let ((symbol (lexer)))
         (when (or (null symbol)
                   (and (not newlines?) (eq #\Newline symbol)))
           (return-from lex (nreverse e)))
         (when (not (eq #\Newline symbol))
           (push symbol e))))))
