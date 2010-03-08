(in-package :simpledb)

;; iterator methods in lisp

(defclass cursor ()
  ((next :initarg :next :accessor next :initform nil)))

(defgeneric cursor-next (cursor)
  (:documentation "Gets the next tuple or nil"))

(defmethod cursor-next ((cursor cursor))
  (with-slots (next) cursor
    (unless (cursor-finished-p cursor)
      (let ((temp next))
        (setf next nil)
        temp))))

(defgeneric cursor-peek (cursor)
  (:documentation "Returns the next tuple, but does not consume it"))

(defmethod cursor-peek ((cursor cursor))
  (with-slots (next) cursor
    (unless (cursor-finished-p cursor)
      (write-debug "Peeking: ~a~%" next)
      next)))

(defgeneric cursor-type-descriptor (cursor)
  (:documentation "Returns the type-descriptor of the tuples returned by this cursor"))

(defgeneric cursor-info (cursor)
  (:documentation "Returns the field info of the tuples returned by this cursor"))

(defgeneric cursor-finished-p (cursor)
  (:documentation "Returns if there are any tuples left. Stores the next tuple in a cache"))

(defgeneric cursor-rewind (cursor)
  (:documentation "Rewinds the cursor back to its original position"))

(defgeneric cursor-open (cursor)
  (:documentation "Initializes cursor"))

(defmethod cursor-open ((cursor cursor))
  (cursor-rewind cursor))

(defgeneric make-cursor-for (collection)
  (:documentation "Creates a cursor for any type of collection"))

;;* page cursors

(defclass page-scan (cursor)
  ((page :initarg :page :accessor page)
   (tuples :initform nil :accessor tuples)))

(defmethod make-cursor-for ((page page))
  (make-instance 'page-scan :page page))

(defmethod cursor-rewind ((cursor page-scan))
  (with-slots (tuples page) cursor
    (setf tuples (copy-list (tuples page)))))

(defmethod cursor-finished-p ((cursor page-scan))
  (with-slots (next tuples) cursor
    (unless (or next (null tuples))
      (write-debug "Caching next tuple from page cursor~%")
      (setf next (pop tuples)))
    (not next)))

;;* file cursors

;;** scan

;; sequentially scan a file for all tuples

(defclass file-scan (cursor)
  ((file :initarg :file :accessor file)
   (current-page-number :initform 0)
   (current-page-cursor :initform nil)))

(defmethod make-cursor-for ((file file))
  (make-instance 'file-scan :file file))

(defmethod cursor-finished-p ((cursor file-scan))
  (with-slots (file next current-page-number current-page-cursor) cursor
    ;; find the cursor to the next page with tuples
    (loop while (and current-page-cursor
                     (cursor-finished-p current-page-cursor)
                     (< current-page-number (page-count file)))
         do
         (setf current-page-cursor 
               (make-cursor-for (read-page file current-page-number)))
         (cursor-open current-page-cursor)
         (incf current-page-number))
    ;; if current-page-cursor is nil, cursor has not been open
    (unless (or next
                (null current-page-cursor)
                (cursor-finished-p current-page-cursor))
      (setf next (cursor-next current-page-cursor)))
    (null next)))

(defmethod cursor-rewind ((cursor file-scan))
  (with-slots (current-page-number current-page-cursor file) cursor
    (setf current-page-number 0)
    (setf current-page-cursor
          (make-cursor-for (read-page file current-page-number)))
    (cursor-open current-page-cursor)
    (incf current-page-number)))
  
(defmethod cursor-type-descriptor ((cursor file-scan))
  (type-descriptor (file cursor)))

(defmethod cursor-info ((cursor file-scan))
  (with-slots (file) cursor
    (let ((table-info (catalog-lookup-info (sxhash (file-name file)))))
      (with-slots (table-name column-names) table-info
        (mapcar #'(lambda (column-name)
                    (cons table-name column-name))
                column-names)))))

;;** join

;; join two cursors using nested loops join

(defclass join (cursor)
  ((inner :initarg :inner :accessor inner)
   (outer :initarg :outer :accessor outer)))

(defmethod cursor-rewind ((cursor join))
  (with-slots (inner outer) cursor
    (cursor-rewind inner)
    (cursor-rewind outer)))

;; does this fail if outer is a cursor that starts out empty?
(defmethod cursor-finished-p ((cursor join))
  (with-slots (next inner outer) cursor
    (unless (or next
                (cursor-finished-p inner))
      (let ((inner-tuple (cursor-peek inner))
            (outer-tuple (cursor-next outer)))
        (setf next (append inner-tuple outer-tuple))
        (write-debug "Join (next): ~a~%" next))
      (when (cursor-finished-p outer)
        (cursor-rewind outer)
        (cursor-next inner)))
    (null next)))

(defmethod cursor-type-descriptor ((cursor join))
  (with-slots (inner outer) cursor
    (let ((type-descriptor (append (cursor-type-descriptor inner)
                                   (cursor-type-descriptor outer))))
      (write-debug "Join (type-descriptor): ~a~%" type-descriptor)
      type-descriptor)))

(defmethod cursor-info ((cursor join))
  (with-slots (inner outer) cursor
    (let ((info (append (cursor-info inner)
                        (cursor-info outer))))
      (write-debug "Join (info): ~a~%" info)
      info)))

(defun join (tables)
  (if (= (length tables) 1)
      (make-cursor-for
       (catalog-lookup-file (cdr (first tables))))
      (make-instance
       'join
       :inner (make-cursor-for
               (catalog-lookup-file (cdr (first tables))))
       :outer (join (rest tables)))))

;;** predicate

(defclass predicate ()
  ((op :initarg :op :accessor op)
   (field :initarg :field :initform 0 :accessor field)
   (value :initarg :value :accessor value)))

;;** filter

;; filter the results of a cursor using a predicate
;; @todo untested

(defclass filter (cursor)
  ((child :initarg :child :accessor child)
   (predicate :initarg :predicate :accessor predicate)))

(defmethod cursor-rewind ((cursor filter))
  (cursor-rewind (child cursor)))

(defmethod cursor-type-descriptor ((cursor filter))
  (cursor-type-descriptor (child cursor)))

(defmethod cursor-info ((cursor filter))
  (cursor-info (child cursor)))

(defmethod cursor-finished-p ((cursor filter))
  (with-slots (child next predicate) cursor
    (loop until (or next (cursor-finished-p child))
         for tuple = (cursor-next child)
         do
         ;; find the next tuple that matches the predicate
         (when (or (not predicate)
                   (case (nth (field predicate) type-descriptor)
                     ('int 
                      (case (op predicate)
                        ('= (= (nth (field predicate) tuple) (value predicate)))
                        ('< (< (nth (field predicate) tuple) (value predicate)))
                        ('> (> (nth (field predicate) tuple) (value predicate)))
                        ('<= (<= (nth (field predicate) tuple) (value predicate)))
                        ('>= (>= (nth (field predicate) tuple) (value predicate)))))
                     ('string
                      (case (op predicate)
                        ('= (string= (nth (field predicate) tuple) (value predicate)))
                        ('< (string< (nth (field predicate) tuple) (value predicate)))
                        ('> (string> (nth (field predicate) tuple) (value predicate)))
                        ('<= (string<= (nth (field predicate) tuple) (value predicate)))
                        ('>= (string>= (nth (field predicate) tuple) (value predicate)))))))
           (setf next tuple)))))

;;** projection

(defclass projection (cursor)
  ((child :initarg :child :accessor child)
   (order :initarg :order :accessor order)))

(defmethod cursor-rewind ((cursor projection))
  (cursor-rewind (child cursor)))

(defmethod cursor-type-descriptor ((cursor projection))
  (with-slots (child order) cursor
    (reorder (cursor-type-descriptor child) order)))

(defmethod cursor-info ((cursor projection))
  (with-slots (child order) cursor
    (reorder (cursor-info child) order)))

(defmethod cursor-finished-p ((cursor projection))
  (with-slots (child next order) cursor
    (unless (cursor-finished-p child)
      (let ((child-tuple (cursor-next child)))
        (setf next (reorder child-tuple order))))
    (null next)))

(defun projection (child fields)
  (write-debug "Fields: ~S~%" fields)
  (write-debug "Child cursor info: ~S~%" (cursor-info child))
  (let* ((child-info (cursor-info child))
         (order
          (flatten
           (if (eq :asterisk fields) nil
               (mapcar 
                #'(lambda (field)
                    (let ((match-position (position field child-info
                                                    :test #'equal)))
                      (if match-position
                          match-position
                          (cond
                            ((and (listp field)
                                  (eq :asterisk (cdr field)))
                             ;; add all fields in this place, flatten
                             ;; will take care of splicing in the list
                             ;; into the order
                             (let (matches)
                               (loop for child-field in child-info
                                  for index from 0 below (length child-info)
                                  when (eq (car child-field) (car field))
                                  do (push index matches))
                               (nreverse matches)))
                            (t
                             ;; try searching using cdr, but check for
                             ;; ambiguity
                             (write-debug "Searching for generic field~%")
                             (let ((match-position-start
                                    (position field
                                              (mapcar #'cdr child-info)
                                              :from-end nil))
                                   (match-position-end
                                    (position field
                                              (mapcar #'cdr child-info)
                                              :from-end t)))
                               (if (eq match-position-start
                                       match-position-end)
                                   (if match-position-end
                                       match-position-end
                                       (error 'field-does-not-exist-error :name field))
                                   (error 'ambiguous-field-error 
                                          :name field))))))))
                fields)))))
    (write-debug "Projection order: ~S~%" order)
    (make-instance 'projection :child child :order order)))

;;** index cursors

;; @todo re-write as a cursor on index-files

;; (defmethod filter ((file index-file) filter callback)
;;   ;; @todo IMPORTANT: assumes key is unique, and hence there is only
;;   ;; one page to fetch from source-file. need to address this issue.
;;   (with-slots (source-file bin-count type-descriptor key-field) file
;;     ;; check if the filter is = and the field is our key-field, if so
;;     ;; we can use the index to filter
;;     (if (and filter (eq (op filter) '=) (eq (field filter) key-field))
;;         ;; use (value filter) to find correct page(s) in the index
;;         (let ((page-number (mod (sxhash (value filter)) bin-count)))
;;           (loop for page = (read-page file page-number)
;;              do
;;                ;; search page for key, and if found, calls filter-page
;;                ;; on the page the tuple can be found
;;                (filter-page page type-descriptor (make-instance 'filter :op '= 
;;                                                                 :value (value filter))
;;                             #'(lambda (tuple type-descriptor record-id)
;;                                 (filter-page (read-page source-file (nth 1 tuple))
;;                                              (type-descriptor source-file) filter callback)))
;;              until (zerop (setf page-number (next-page-number page)))))
;;         ;; defer to the source-file to see if it can do any better
;;         (filter source-file filter callback))))
