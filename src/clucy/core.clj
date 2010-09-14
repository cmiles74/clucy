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

(def *version*  Version/LUCENE_30)
(def *analyzer* (StandardAnalyzer. *version*))
(def *optimize-frequency* 1)
(def *merge-factor* 10)
(def *ram-buffer-size* IndexWriter/DEFAULT_RAM_BUFFER_SIZE_MB)
(def *compound-file* true)
(def *optimize-max-num-segments* 1)

(defstruct
    #^{:doc "Structure for clucy indexes."}
    clucy-index :index :optimize-frequency :updates)

;; flag to indicate a default "_content" field should be maintained
(def *content* true)

(defn- initialize-index
  [index]
  (swap! index merge {:optimize-frequency *optimize-frequency*
                      :merge-factor *merge-factor*
                      :compound-file *compound-file*
                      :ram-buffer-size *ram-buffer-size*
                      :optimize-max-num-segments *optimize-max-num-segments*
                      :updates 0})
  index)

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

(defn- index-writer
  "Create an IndexWriter."
  [index]
  (let [writer (IndexWriter. (:index @index)
                             *analyzer*
                             IndexWriter$MaxFieldLength/UNLIMITED)]
    (.setMergeFactor writer (:merge-factor @index))
    (.setUseCompoundFile writer (:compound-file @index))
    (.setRAMBufferSizeMB writer (:ram-buffer-size @index))
    writer))

(defn- optimize-index
  "Optimized the provided index if the number of updates matches or
  exceeds the optimize frequency."
  [index]
  (if (<= (:optimize-frequency @index) (:updates @index))
    (with-open [writer (index-writer index)]
      (.optimize writer (:optimize-max-num-segments @index))
      (swap! index assoc :updates 0))
    index))

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

(defn add
  "Add hash-maps to the search index."
  [index & maps]
  (with-open [writer (index-writer index)]
    (doseq [m maps]
      (swap! index assoc :updates (inc (:updates @index)))
      (.addDocument writer (map->document m))))
  (optimize-index index))

(defn delete
  "Deletes hash-maps from the search index."
  [index & maps]
  (with-open [writer (index-writer index)]
    (doseq [m maps]
      (let [query (BooleanQuery.)]
        (doseq [[key value] m]
          (.add query
                (BooleanClause.
                 (TermQuery. (Term. (.toLowerCase (as-str key))
                                    (.toLowerCase (as-str value))))
                 BooleanClause$Occur/MUST)))
        (.deleteDocuments writer query))
      (swap! index assoc :updates (inc (:updates @index)))))
  (optimize-index index))

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

(defn search
  "Search the supplied index with a query string."
  ([index query max-results]
     (if *content*
       (search index query max-results :_content)
       (throw (Exception. "No default search field specified"))))
  ([index query max-results default-field]
    (with-open [searcher (IndexSearcher. (:index @index))]
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
    (with-open [writer (index-writer index)]
      (let [parser (QueryParser. *version* (as-str default-field) *analyzer*)
            query  (.parse parser query)]
        (.deleteDocuments writer query)
        (swap! index assoc :updates (inc (:updates @index)))))
    (optimize-index index)))
