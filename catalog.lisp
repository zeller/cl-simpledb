(in-package :simpledb)

;;** catalog
  
;; table-info keeps track of the table name and the column names, 
;; which are stored in the catalog
;; note: not currently used yet
(defclass table-info ()
  ((table-name :initarg :table-name :accessor table-name)
   (column-names :initarg :column-names :accessor column-names)))

;; catalog stores the information about tables via a mapping to a table-number
(defclass catalog ()
  ((table-file :initform (make-hash-table) :accessor table-file)
   (table-indexes :initform (make-hash-table) :accessor table-indexes)
   (table-info :initform (make-hash-table) :accessor table-info)
   (table-numbers :initform (make-hash-table) :accessor table-numbers)))

(defun clear-catalog ()
  (setf *catalog* (make-instance 'catalog)))

;; file methods

(defgeneric catalog-add-file (table-number file)
  (:documentation "Addes a file to the catalog and maps it to a table."))

(defun catalog-lookup-file (table-number)
  (gethash table-number (table-file *catalog*)))

(defmethod catalog-add-file (table-number (file file))
  (setf (gethash table-number (table-file *catalog*)) file))

(define-condition table-does-not-exist-error (error)
  ((name :initarg :name :reader name)))

(defun catalog-lookup-table-number (table-name)
  (let ((table-number (gethash table-name (table-numbers *catalog*))))
    (if (null table-number)
        (error 'table-does-not-exist-error :name table-name)
        table-number)))

;; info methods

(defgeneric catalog-add-info (table-number info)
  (:documentation "Adds information about a file to the catalog."))

(defun catalog-lookup-info (table-number)
  (gethash table-number (table-info *catalog*)))

(defmethod catalog-add-info (table-number (info table-info))
  (setf (gethash table-number (table-info *catalog*)) info)
  (setf (gethash (table-name info) (table-numbers *catalog*)) table-number))

;; indexing methods

(defgeneric catalog-add-index (table-number index)
  (:documentation "Adds an index to the catalog and maps it to a table."))

;; @todo return a list of available indexes
(defun catalog-lookup-index (table-number)
  (gethash table-number (table-indexes *catalog*)))

;; @todo allow more than one index on a table and key-field
(defmethod catalog-add-index (table-number (index index))
  (setf (gethash table-number (table-indexes *catalog*)) index))

(defvar *catalog* (make-instance 'catalog)
  "Catalog of information about the database")
