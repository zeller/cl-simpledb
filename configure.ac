#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.59)
AC_INIT(Common Lisp SimpleDB, 0.1, me@michaelzeller.com)

AC_SUBST(LISP_PROGRAM)
AC_SUBST(LISP)

# Checks for programs.
AC_ARG_WITH(sbcl, [ --with-sbcl=PATH      specify location of sbcl], SBCL_PATH=$withval, SBCL_PATH="")

if test -x "$SBCL_PATH"; then
   SBCL=$SBCL_PATH
   AC_MSG_CHECKING([for sbcl])
   AC_MSG_RESULT($SBCL)
else
   AC_PATH_PROG([SBCL], sbcl,"")
fi

LISP=sbcl
LISP_PROGRAM=$SBCL

if test "x$LISP_PROGRAM" = "x"; then
   AC_MSG_ERROR([*** No lisp is available.])
fi

AC_MSG_NOTICE([Using $LISP at $LISP_PROGRAM])

# Checks for libraries.

# Checks for header files.

# Checks for typedefs, structures, and compiler characteristics.

# Checks for library functions.
AC_OUTPUT(Makefile)
AC_OUTPUT(make-image.lisp)
