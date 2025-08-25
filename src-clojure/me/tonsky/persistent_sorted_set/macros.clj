(ns me.tonsky.persistent-sorted-set.macros
  (:refer-clojure :exclude [async await])
  (:require [clojure.walk]
            ;[clojure.pprint :refer [pprint]]
            [await-cps :refer [terminators]]
            [await-cps.ioc :refer [has-terminators?] :as ioc]
            ;[cljs.analyzer :as ana]
            [riddley.walk :refer [macroexpand-all]]))

(def async->sync '{async do, await do})

(defmacro async+sync [sync? async-code]
  (let [sync-code      (clojure.walk/postwalk
                         (fn [n] (async->sync n n))
                         async-code)]
    `(if ~sync?
       ~sync-code
       ~async-code)))

;(defn await-terminators [env]
;  (let [await-q (or (await-cps.ioc/var-name env 'await) 'await)] ; fallback
;    {await-q 'await-cps/do-await}))
;
;(defn async-sexp
;  "Returns a unified CPS function that decides at call time whether to invoke callbacks synchronously or asynchronously.
;   If body contains no await calls, calls resolve callback synchronously.
;   If body contains await calls, returns a CPS function that may call callbacks asynchronously."
;  [env body]
;  (let [terms (await-terminators env)
;        ctx {:terminators terms
;             :env env}
;        has-await? (has-terminators? body ctx)]
;    (if has-await?
;      (let [r (gensym) e (gensym)
;            params {:r r :e e :env env :terminators terms}
;            expanded (macroexpand-all (cons 'do body))]
;        `(fn [~r ~e]
;           (await-cps/->thunk
;             (fn []
;               ;; TODO  This is not handling await correct
;               ~(ioc/invert params expanded)))))
;      `(fn [r# e#]
;         (try
;           (let [result# (do ~@body)]
;             (r# result#))
;           (catch ~(if (:js-globals env) :default `Throwable) t#
;             (e# t#)))))))
;
;(def *cache (atom {}))
;
;(defn expand-async-topdown [env form]
;  (clojure.walk/prewalk
;    (fn [n]
;      (if (and (seq? n) (= 'async (first n)))
;        (let [hit (get @*cache n)]
;          (if hit
;            (do
;              (println "HIT")
;              hit)
;            (let [_ (println "MISS" n)
;                  exp (async-sexp env (rest n))]
;              (swap! *cache assoc n exp)
;              exp)))
;        n))
;    form))
;
;(defmacro async+sync [sync? async-code]
;  (let [expanded-async (expand-async-topdown &env async-code)
;        sync-code      (clojure.walk/postwalk
;                         (fn [n] (async->sync n n))
;                         expanded-async)]
;    `(if ~sync?
;       ~sync-code
;       ~expanded-async)))

