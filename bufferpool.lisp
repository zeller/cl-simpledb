(in-package :simpledb)

;;** bufferpool

(defclass bufferpool ()
  ((pool :initform (make-hash-table) :reader pool)
   (front :initform nil :accessor front)
   (back :initform nil :accessor back)
   (queue-length :initform 0 :accessor queue-length)))

(defun clear-bufferpool ()
  (loop while (front *bufferpool*) do 
       (evict-page-from-bufferpool))
  (setq *bufferpool* (make-instance 'bufferpool)))

(defun fetch-from-bufferpool (id)
    (gethash id (pool *bufferpool*)))

(defun add-to-bufferpool (id page)
  (write-debug "Attempting to add page (~a) to the bufferpool~%" id)
  ;; if full, evict a page
  (with-slots (queue-length pool front back) *bufferpool*
    (when (= queue-length *bufferpool-max-size*)
      (evict-page-from-bufferpool))
    ;; add page
    (setf (gethash id pool) page)
    (let ((id-node (cons id nil)))
      (if (zerop queue-length)
          (setf front id-node)
          (setf (cdr back) id-node))
      (setf back id-node))
    (incf queue-length)))

(defun evict-page-from-bufferpool ()
  (with-slots (queue-length pool front back) *bufferpool*
    (let* ((evicted-id (pop front))
           (evicted-page (gethash evicted-id pool)))
      (write-debug "Evicting page (~a) from the bufferpool~%" evicted-id)
      ;; write evicted-page to disk and remove entry
      (with-slots (record-id dirty?) evicted-page
        (when dirty?
          (write-page (catalog-lookup-file (table-number record-id))
                      evicted-page)))
      (remhash evicted-id pool)
      (when (not front)
        (setf back nil))
      (decf queue-length))))

(defvar *bufferpool* (make-instance 'bufferpool)
  "Buffer of pages")
