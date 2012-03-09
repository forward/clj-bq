(ns clj-bq.core
  ( :import [com.google.api.client.googleapis.auth.oauth2.draft10 GoogleAuthorizationRequestUrl GoogleAccessProtectedResource]
            [com.google.api.client.http.javanet NetHttpTransport]
            [com.google.api.client.http HttpRequestFactory]
            [com.google.api.client.http.json JsonHttpRequestInitializer]
            [com.google.api.client.json.jackson JacksonFactory]
            [com.google.api.services.bigquery Bigquery]
            [com.google.api.services.bigquery.model Job JobConfiguration JobConfigurationQuery]))

(def ^:dynamic *credentials* nil)
(def ^:dynamic *authentication-code* nil)
(def ^:dynamic *callback-url* "urn:ietf:wg:oauth:2.0:oob")
(def ^:dynamic *auth-response* nil)

(defn auth-url []
  (let [auth-req (GoogleAuthorizationRequestUrl. (:client-id *credentials*)
                                                 *callback-url*
                                                 (:scope *credentials*))]
    (.build auth-req)
    (.toString auth-req)))

(defn set-authentiation-code! [code]
  (alter-var-root #'*authentication-code* (constantly code)))

(defn set-credentials! [credential]
  (alter-var-root #'*credentials* (constantly credentials)))

(defn set-auth-response! [auth-resp]
  (alter-var-root #'*auth-response* (constantly auth-resp)))

(defn start-authentication [credentials]
  (set-credentials! credentials)
  (println (str "* browse to this URL " (auth-url)  " and generate an authentication code ")))

(defn continue-authentication [auth-code]
  (set-authentiation-code! auth-code)
  (let [net-transport (NetHttpTransport.)
        json-factory (JacksonFactory.)
        auth-req (com.google.api.client.googleapis.auth.oauth2.draft10.GoogleAccessTokenRequest$GoogleAuthorizationCodeGrant.
                  net-transport
                  json-factory
                  (:client-id *credentials*)
                  (:client-secret *credentials*)
                  *authentication-code*
                  *callback-url*)]
    (set! (. auth-req :useBasicAuthorization) false)
    (let [auth-resp (.execute auth-req)]
      (set-auth-response! auth-resp))))

(defn protected-request-factory []
  (let [access-token (. *auth-response* :accessToken)
        net-transport (NetHttpTransport.)
        json-factory (JacksonFactory.)
        access (GoogleAccessProtectedResource.
                access-token
                net-transport
                json-factory
                (:client-id *credentials*)
                (:client-secret *credentials*)
                (. *auth-response* :refreshToken))]
    ;(.createRequestFactory net-transport access)
    access))



(defn big-query []
  (let [net-transport (NetHttpTransport.)
        json-factory (JacksonFactory.)
        bq (com.google.api.services.bigquery.Bigquery/builder net-transport json-factory)]
    (.setHttpRequestInitializer bq (protected-request-factory))
    (.build bq)))


(defn project-id-for-name [bq name]
  (:id (first (filter (fn [proj] (= (:name proj) name)) (projects bq)))))

(defn datasets [bq project-name]
  (let [project-id (project-id-for-name bq project-name)
        datasets-list (.execute (.list (.datasets bq) project-id))]
    (map (fn [ds]
           {:id (.getId ds)
            :kind (.getKind ds)
            :name (.getFriendlyName ds)
            :reference (.getDatasetReference ds)})
         (vec (.getDatasets  datasets-list)))))

(defn projects [bq]
  (let [projects-list (.execute (.list (.projects bq)))]
    (map (fn [proj]
           {:id (.getId proj)
            :kind (.getKind proj)
            :name (.getFriendlyName proj)
            :reference (.getProjectReference proj)})
         (vec (.getProjects projects-list)))))


(defn jobs [bq project-name]
  (let [project-id (project-id-for-name bq project-name)
        jobs-list (.execute (.list (.jobs bq) project-id))]
    (map (fn [job]
           {:id (.getJobId (.getJobReference job))
            ;:kind (.getKind job)
            :state (.getState job)
            :statistics (.getStatistics job)
            :reference (.getJobReference job)})
         (vec (.getJobs jobs-list)))))


(defn async-job [bq project-name query-sql]
  (let [job (Job.)
        project-id (project-id-for-name bq project-name)
        config (JobConfiguration.)
        query-config (JobConfigurationQuery.)]
    (.setQuery config query-config)
    (.setConfiguration job config)
    (.setQuery query-config query-sql)
    (let [insert (.insert (.jobs bq) job)]
      (.setProjectId insert project-id)
      (.. insert execute getJobReference))))


(defn job-results [bq project-name job-id]
  (let [jobs (.jobs bq)
        project-id (project-id-for-name bq project-name)
        query-result (.execute (.getQueryResults jobs project-id job-id))]
    (.getRows query-result)))

(defn as-tuples [row]
  (reduce (fn [ac v]
            (conj ac (.getV v)))
          []
          (.getF row)))


;; utils

(defn show-java-methods
  "Collections and optionally prints the methods defined in a Java object"
  ([obj should-show?]
     (let [ms (.. obj getClass getDeclaredMethods)
           max (alength ms)]
       (loop [count 0
              acum []]
         (if (< count max)
           (let [m (aget ms count)
                 params (.getParameterTypes m)
                 params-max (alength params)
                 return-type (.getReturnType m)
                 to-show (str (loop [acum (str (.getName m) "(")
                                     params-count 0]
                                (if (< params-count params-max)
                                  (recur (str acum " " (aget params params-count))
                                         (+ params-count 1))
                                  acum))
                              " ) : " return-type)]
             (when should-show? (println (str to-show)))
             (recur (+ 1 count)
                    (conj acum (str to-show))))
           acum))))
  ([obj] (show-java-methods obj true)))
