;; -*- mode: lisp -*-

(defpackage :simpledb-system
  (:use :cl :asdf))

(in-package :simpledb-system)

(defsystem :simpledb
  :name "SimpleDB"
  :author "Michael Zeller <me@michaelzeller.com>"
  :version "0.1"
  :maintainer "Michael Zeller <me@michaelzeller.com>"
  ;; :license "GNU General Public License"
  :description "A Common Lisp implementation of SimpleDB @ MIT"
  :serial t
  :depends-on (:cl-ppcre :fucc-parser :fucc-generator :flexi-streams :sb-posix)
  :components ((:file "package")
	       (:file "utils")
               (:file "parameters")
               (:file "lexer")
               (:file "parser")
               (:file "file")
               (:file "cursor")
	       (:file "bufferpool")
	       (:file "catalog")
	       (:file "simpledb")))
