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
   (table-info :initform (make-hash-table) :accessor table-info)))

(defun clear-catalog ()
  (setf *catalog* (make-instance 'catalog)))

;; info methods

;; @todo add methods for setting and getting the info (names) of a table

;; file methods

(defgeneric catalog-add-file (table-number file)
  (:documentation "Addes a file to the catalog and maps it to a table."))

(defun catalog-lookup-file (table-number)
  (gethash table-number (table-file *catalog*)))

(defmethod catalog-add-file (table-number (file file))
  (setf (gethash table-number (table-file *catalog*)) file))

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
