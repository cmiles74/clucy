(ns clucy.core
  (:use clojure.contrib.java-utils)
  (:import java.io.File
           org.apache.lucene.document.Document
           (org.apache.lucene.document Field Field$Store Field$Index)
           (org.apache.lucene.index IndexWriter IndexWriter$MaxFieldLength)
           org.apache.lucene.analysis.standard.StandardAnalyzer
           org.apache.lucene.queryParser.QueryParser
           org.apache.lucene.search.IndexSearcher
           (org.apache.lucene.store RAMDirectory NIOFSDirectory)
           org.apache.lucene.util.Version
           org.apache.lucene.search.BooleanQuery
           org.apache.lucene.search.BooleanClause
           org.apache.lucene.search.BooleanClause$Occur
           org.apache.lucene.index.Term
           org.apache.lucene.search.TermQuery))

;; the version of the Lucene API to emulate
(def *version*  Version/LUCENE_30)

;; analyzer to use when parsing data
(def *analyzer* (StandardAnalyzer. *version*))

;; the number of index updates required before optimization
(def *optimize-frequency* 1)

;; number of segments to merge at any one time
(def *merge-factor* 10)

;; flag to indicate if the compound index format should be used
(def *compound-file* true)

;; the number of segments to optimize the index down to
(def *optimize-max-num-segments* 1)

;; flag to indicate a default "_content" field should be maintained
(def *content* true)

(defstruct
    #^{:doc "Structure for clucy indexes."}
  clucy-index
    :index :index-writer :index-searcher :optimize-frequency
    :merge-factor :compound-file :ram-buffer-size :optimiaze-max-num-segments :updates)

(defn- initialize-index
  "Updates the index to reflect the current settings."
  [index]
  (swap! index merge {:optimize-frequency *optimize-frequency*
                      :merge-factor *merge-factor*
                      :compound-file *compound-file*
                      :optimize-max-num-segments *optimize-max-num-segments*
                      :updates 0})
  index)

(defn- create-index-writer
  "Creates a new IndexWriter for the provided index."
  [index]
   (doto (IndexWriter. (:index @index)
                       *analyzer*
                       IndexWriter$MaxFieldLength/UNLIMITED)
     (.setMergeFactor (:merge-factor @index))
     (.setUseCompoundFile (:compound-file @index))))

(defn- index-writer
  "Returns the IndexWriter for the index or creates a new IndexWriter
  if one isn't present."
  [index]
  (if (:index-writer @index)
    (:index-writer @index)
    (:index-writer (swap! index assoc :index-writer
                          (create-index-writer index)))))

(defn- create-index-searcher
  "Creates a new IndexSearcher for the provided index."
  [index]
  (IndexSearcher. (:index @index)))

(defn- index-searcher
  "Returns the IndexSearcher for the index or creates a new
  IndexSearcher if the current one is out-of-date."
  [index]
  (if (and (:index-searcher @index)
           (.isCurrent (.getIndexReader (:index-searcher @index))))
    (:index-searcher :index)
    (:index-searcher (swap! index assoc :index-searcher
                            (create-index-searcher index)))))

(defn- close-index
  "Closes the index to further writing."
  [index]
  (if (:index-writer @index)
    (do (.close (:index-writer @index))))
  (swap! index merge {:index-writer nil
                      :index-searcher nil})
  index)

(defn- optimize-index
  "Optimizes the provided index if the number of updates matches or
  exceeds the optimize frequency."
  [index]
  (if (<= (:optimize-frequency @index) (:updates @index))
    (do
      (let [writer (index-writer index)]
        (.commit writer)
        (.optimize writer (:optimize-max-num-segments @index)))
      (close-index index)))
  index)

(defn- add-field
  "Add a Field to a Document."
  ([document key value]
     (add-field document key value {}))

  ([document key value meta-map]
       (.add document
             (Field. (as-str key) (as-str value)
                     (if (and meta-map (= false (:stored meta-map)))
                       Field$Store/NO
                       Field$Store/YES)
                     (if (and meta-map (= false (:indexed meta-map)))
                       Field$Index/NO
                       Field$Index/ANALYZED)))))

(defn- map-stored
  "Returns a hash-map containing all of the values in the map that
  will be stored in the search index."
  [map-in]
  (merge {}
         (filter (complement nil?)
                 (map (fn [item]
                        (if (or (= nil (meta map-in))
                                (not= false
                                      (:stored ((first item) (meta map-in)))))
                          item)) map-in))))

(defn- concat-values
  "Concatenate all the maps values being stored into a single string."
  [map-in]
  (apply str (interpose " " (vals (map-stored map-in)))))

(defn- map->document
  "Create a Document from a map."
  [map]
  (let [document (Document.)]
    (doseq [[key value] map]
      (add-field document key value (key (meta map))))
    (if *content*
      (add-field document :_content (concat-values map)))
    document))

(defn- document->map
  "Turn a Document object into a map."
  [document]
  (with-meta
    (-> (into {}
              (for [f (.getFields document)]
                [(keyword (.name f)) (.stringValue f)]))
        (dissoc :_content))
    (-> (into {}
              (for [f (.getFields document)]
                [(keyword (.name f))
                 {:indexed (.isIndexed f)
                  :stored (.isStored f)
                  :tokenized (.isTokenized f)}]))
        (dissoc :_content))))

(defn memory-index
  "Create a new index in RAM."
  []
  (initialize-index
   (atom (struct-map clucy-index
           :index (RAMDirectory.)))))

(defn disk-index
  "Create a new index in a directory on disk."
  [dir-path]
  (initialize-index
   (atom (struct-map clucy-index
           :index (NIOFSDirectory. (File. dir-path))))))

(defn add
  "Add hash-maps to the search index."
  [index & maps]
  (doseq [m maps]
    (.addDocument (index-writer index) (map->document m))
    (swap! index assoc :updates (inc (:updates @index)))
    (optimize-index index))
  (.commit (index-writer index))
  (optimize-index index))

(defn delete
  "Deletes hash-maps from the search index."
  [index & maps]
  (doseq [m maps]
    (let [query (BooleanQuery.)]
      (doseq [[key value] m]
        (.add query
              (BooleanClause.
               (TermQuery. (Term. (.toLowerCase (as-str key))
                                  (.toLowerCase (as-str value))))
               BooleanClause$Occur/MUST)))
      (.deleteDocuments (index-writer index) query)
      (.commit (index-writer index)))
    (swap! index assoc :updates (inc (:updates @index)))
    (optimize-index index)))

(defn search
  "Search the supplied index with a query string."
  ([index query max-results]
     (if *content*
       (search index query max-results :_content)
       (throw (Exception. "No default search field specified"))))
  ([index query max-results default-field]
    (let [searcher (index-searcher index)]
      (let [parser (QueryParser. *version* (as-str default-field) *analyzer*)
            query  (.parse parser query)
            hits   (.search searcher query max-results)]
        (doall
          (for [hit (.scoreDocs hits)]
            (document->map (.doc searcher (.doc hit)))))))))

(defn search-and-delete
  "Search the supplied index with a query string and then delete all
of the results."
  ([index query]
     (if *content*
       (search-and-delete index query :_content)
       (throw (Exception. "No default search field specified"))))
  ([index query default-field]
     (let [parser (QueryParser. *version* (as-str default-field) *analyzer*)
           query  (.parse parser query)]
       (.deleteDocuments (index-writer index) query)
       (.commit (index-writer index))
       (swap! index assoc :updates (inc (:updates @index)))
       (optimize-index index))))
