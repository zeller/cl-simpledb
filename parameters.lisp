(in-package :simpledb)

;;** parameters

(defparameter *max-int-size* 4294967296
  "The maximum storable INT")

(defparameter *int-size* (ceiling (/ (ceiling (log *max-int-size* 2)) 8))
  "The number of bytes needed for storing an int")

(defparameter *max-table-size* 2147483648
  "The max allowable size of a table (file) on disk")

(defparameter *page-size* 32 ;; 65536
  "The size in bytes of a page.")

(defparameter *index-bin-count* 10
  "Number of bins for static hashing in index.")

(defparameter *max-pages* (floor (/ *max-table-size* *page-size*))
  "The maximum allowable number of pages, computed from *max-table-size*")

(defparameter *page-offset-size* (ceiling (/ (ceiling (log *max-pages* 2)) 8))
  "The size in bytes of the offset storing a page number.")

(defparameter *offset-size* (ceiling (/ (ceiling (log *page-size* 2)) 8))
  "The size in bytes of the offsets within a page.")

(defparameter *bufferpool-max-size* 50
  "Number of pages in the bufferpool")
