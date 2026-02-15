(ns org.replikativ.persistent-sorted-set.test.macros)

(defmacro testing-group [string & body]
  `(do
     (~'js/console.group ~string)
     (~'cljs.test/update-current-env! [:testing-contexts] conj ~string)
     (try
       ~@body
       (finally
         (~'js/console.groupEnd ~string)
         (~'cljs.test/update-current-env! [:testing-contexts] rest)))))
