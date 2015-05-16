;; Copyright Cognitect, Inc. All rights reserved.

(ns com.datomic.validate-cas
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import java.io.Reader))

(defn read-all
  "Return a vector of all forms in src. Closes reader."
  [^Reader src]
  (with-open [src src]
    (let [reader (java.io.PushbackReader. src)]
      (loop [tv (transient [])]
        (let [form (edn/read {:eof tv} reader)]
          (if (identical? tv form)
            (persistent! tv)
            (recur (conj! tv form))))))))

(defn sorted-swaps
  "Given a collection of CAS results

     [result initial-val propsed-val thread-id proc-id]

   returns a collection of those items whose result is
   :success, sorted by initial-val."
  [cas-results]
  (->> cas-results
       (filter #(= :success (first %)))
       (sort-by second)))

(defn gaps
  "Given a collection of sorted-swaps, returns gap pairs
   where a swap appears to be missing.  This can happen
   without violating consistency if a swap succeeded but
   an error was reported to the client."
  [ss]
  (->> ss
       (partition 2 1)
       (filter (fn [[[_ ival1] [_ ival2]]]
                 (< 1 (- ival2 ival1))))
       seq))

(defn dups
  "Given a collection of sorted-swaps, returns pairs
   with duplicate start values. This is a consistency
   violation."
  [ss]
  (->> ss
       (partition 2 1)
       (filter (fn [[[_ ival1] [_ ival2]]]
                 (= ival1 ival2)))
       seq))

(defn disordered
  "Given a collection of CAS results

     [result initial-val propsed-val thread-id proc-id]

   returns a map of

     [thread-id proc-id] => pairs of out-of-order results

   or nil if no threads saw anything our of order.
   Non-nil result indicates a consistency failure."
  [results]
  (reduce-kv
   (fn [m k thread-result]
     (if-let [t (->> (partition 2 1 thread-result)
                     (filter (fn [[[_ v1] [_ v2]]] (<= v2 v1)))
                     seq)]
       (assoc m k t)
       m))
   nil
   (group-by (fn [[_ _ _ tid pid]] [tid pid]) results)))

(defn validate-file
  [f]
  (let [r (io/reader f)
        results (read-all r)
        swaps (sorted-swaps results)
        d (dups swaps)
        dis (disordered results)]
    {:valid? (and (not d) (not dis))
     :attempts (count results)
     :successes (count swaps)
     :gaps (gaps swaps)
     :dups d
     :disordered dis}))


