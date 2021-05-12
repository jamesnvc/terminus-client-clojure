(ns terminus.core
  "Clojure client for TerminusDB"
  (:require
   [clojure.data.json :as json]
   [clojure.java.io :as io]
   [clojure.set :as set]
   [clojure.string :as string]
   [org.httpkit.client :as http]
   [taoensso.timbre :as timbre])
  (:import
   (java.nio.charset StandardCharsets)
   (java.util Base64)))

(defn base64
  [s]
  (-> s
      (.getBytes "utf-8")
      (->> (.encode (Base64/getEncoder)))
      (String. StandardCharsets/UTF_8)))

(defn parse-query
  "Take a query as clojure data & convert it to WOQL.
  https://terminusdb.com/docs/terminushub/reference/server/woql/"
  [q]
  (let [type (first q)
        [vars more] (split-with (complement keyword?) (rest q))
        [ins [_ & query]]
        (if (= :in (first more))
          (split-with (complement keyword?) (rest more))
          [[] more])]
    {:type type
     :vars vars
     :in ins
     :query query}))

(comment

  (parse-query
    '[:find ?x ?y
      :in $ ?foo
      :where
      [?x :thing ?y]
      [?y :beep ?foo]])

  )

(defprotocol TerminusClient
  (_connect [_ args])
  (_close [_])
  (_get-database [_ db-id account])
  (_get-databases [_])
  (_create-database [_ {:keys [db-id account-id label description prefixes
                               include-schema?]}])
  (_query [this {:keys [query commit-msg file-dict]}])

  (__dispatch [_ {:keys [action url payload file-list]}])
  (__get-prefixes [_])
  (__db-base [_ action])
  (__query-url [_])
  (__branch-base [_ action])
  (__repo-base [_ action])
  )

(defrecord ClientImpl [server-url api connected? insecure? conn-info]
  TerminusClient

  (_connect [this args]
    (reset! conn-info args)
    (reset! connected? true)
    (let [capabilities (json/read-str (.__dispatch this {:action :get :url api}))]
      (swap! conn-info assoc :uid (get capabilities "@id"))
      (swap! conn-info assoc :author (get-in capabilities ["system:user_identifier" "@value"]
                                             (:user args))))
    (when (some? (:db args))
      (swap! conn-info assoc :context (.__get-prefixes this)))

    true)

  (_close [_]
    (reset! connected? false))

  (_get-databases [this]
    (when-not @connected?
      (throw (ex-info "Not connected to a server" {:client this})))
    (-> (.__dispatch this {:action :get :url api})
        json/read-str
        (get-in ["system:role" "system:capability" "system:capability_scope"])
        (->> (filter (fn [scope] (= "system:Database" (get scope "@type")))))
        doall))

  (_get-database [this db-id account]
   (when-not @connected?
     (throw (ex-info "Not connected to a server" {:client this})))

    (let [dbs (->> (._get-databases this)
                   (filter (fn [db] (= db-id (get-in db ["system:resource_name" "@value"]))))
                   doall)
          resource-ids (->
                         (.__dispatch this {:action :get :url api})
                         json/read-str
                         (get-in ["system:role" "system:capability" "system:capability_scope"])
                         (->> (filter
                                (fn [scope]
                                  (and (= "system:Organization" (get scope "@type"))
                                       (= account (get-in scope ["system:organization_name" "@value"]))
                                       (coll? (get scope "system:resource_includes")))))
                              (mapcat (fn [scope]
                                        (map #(get % "@id")
                                             (get scope "system:resource_includes"))))
                              (into #{})))
          target-id (-> (set/intersection
                          (into #{} (map #(get % "@id") dbs))
                          resource-ids)
                        first)]
      (->> dbs (filter (fn [db] (= target-id (get db "@id")))) first)))

  (_create-database [this {:keys [db-id account-id label description prefixes
                                  include-schema?]}]
    (when-not @connected?
      (throw (ex-info "Not connected to a server" {:client this})))

    (swap! conn-info update :account (fnil identity account-id))
    (swap! conn-info assoc :db db-id)

    (.__dispatch this {:action :post
                       :url (.__db-base this "db")
                       :payload (cond-> {:label (or label db-id)
                                         :comment (or description "")}
                                  include-schema? (assoc :schema true)
                                  (some? prefixes) (assoc :prefixes prefixes))})

    (swap! conn-info assoc :context (.__get-prefixes this))

    nil)

  (_query [this {:keys [query commit-msg files]}]
    (when-not @connected?
      (throw (ex-info "Not connected to a server" {:client this})))

    (->> {:action :post
          :url (.__query-url this)
          :payload {:commit_info {:author (:author @conn-info)
                               :message (or commit-msg "Commit via Clojure client")}
                    :query (-> (parse-query query)
                            (assoc "@context" (:context @conn-info)))}
          :file-list files}
         (._dispatch this)
         json/read-str))

  (__dispatch [_ {:keys [action url payload file-list]
                  :or {payload {} file-list []}}]
    (let [verify? (not (or insecure?
                           (string/starts-with? url "http://")
                           (string/starts-with? url "https://127.0.0.1")
                           (string/starts-with? url "https://localhost")))
          basic-auth (and @connected?
                          (some? (:key @conn-info))
                          (some? (:user @conn-info))
                          (str (:user @conn-info) ":" (:key @conn-info)))
          remote-auth (and @connected? (:remote-auth @conn-info))
          headers (cond-> {}
                    basic-auth
                    (assoc "Authorization" (str "Basic " (base64 basic-auth)))

                    (and remote-auth (= (remote-auth "type") "jwt"))
                    (assoc "Authorization-Remote" (str "Bearer " (remote-auth "key")))

                    (and remote-auth (= (remote-auth "type") "basic"))
                    (assoc "Authorization-Remote"
                           (->> (str (remote-auth "user") ":" (remote-auth "key"))
                                base64 (str "Basic "))))
          resp @(http/request (cond-> {:url url
                                       :method action
                                       :headers headers
                                       :insecure? (not verify?)}
                                (and (= action :get) (some? payload))
                                (assoc :query-params payload)

                                (= action :delete)
                                (-> (assoc :body (json/write-str payload))
                                    (assoc-in [:headers "Content-Type"] "application/json"))

                                (and (#{:put :post} action)
                                     file-list)
                                (assoc :multipart (->> (for [path file-list
                                                             :let [f (io/file path)]]
                                                         {:name (.getName f)
                                                          :content f
                                                          :content-type "application/binary"})
                                                       (into [{:name "payload"
                                                               :content (json/write-str payload)
                                                               :content-type "application/json"}])))
                                (and (#{:put :post} action)
                                     (not file-list))
                                (-> (assoc :body (json/write-str "payload"))
                                    (assoc-in [:headers "Content-Type"] "application/json"))))]
      #_(timbre/info "response" resp)
      (if (< 399 (:status resp) 599)
        (throw (ex-info "Bad request" resp))
        (:body resp))))

  (__get-prefixes [this]
    (-> (.__dispatch this {:action :get
                          :url (.__db-base this :prefixes)})
        json/read-str
        (get "@context")))

  (__db-base [_ action]
    (str api "/" (name action) "/"
         (if (= (:db @conn-info) "_system")
           (:db @conn-info)
           (str (:account @conn-info) "/" (:db @conn-info)))))

  (__query-url [this]
    (if (= "_system" (:db @conn-info))
      (.__db-base this "woql")
      (.__branch-base this "woql")))

  (__branch-base [this action]
    (cond
      (= "_meta" (:repo @conn-info))
      (.__repo-base this action)

      (= "_commits" (:branch @conn-info))
      (str (.__repo-base this action) "/" (:branch @conn-info))

      (some? (:ref @conn-info))
      (str (.__repo-base this action) "/commit/" (:ref @conn-info))

      :else
      (str (.__repo-base this action) "/branch/" (:branch @conn-info))))

  (__repo-base [this action]
    (str (.__db-base action) "/" (:repo @conn-info)))

  )

(defn client
  ([url] (client url false))
  ([url insecure?]
   (let [url (string/replace url #"[/]$" "")]
     (map->ClientImpl {:server-url url
                       :api (str url "/api")
                       :connected? (atom false)
                       :insecure? insecure?
                       :conn-info (atom nil)}))))

(defn connect
  [^ClientImpl client
   {:keys [account db remote-auth key user branch ref repo]
    :or {account "admin" key "root" user "admin" branch "main" repo "local"}
    :as args}]
  (._connect client args))

(defn create-database
  [client params]
  (._create-database client params))

(defn get-databases
  [client]
  (._get-databases client))

(defn get-database
  [client db-id account-id]
  (._get-database client db-id account-id))

(defn execute
  [client query]

  )

(comment

  (def test-client (client "https://localhost:6363"))

  (connect test-client {:key "foobarbaz" :account "admin" :user "admin"
                        :db "testingone"})


  (._get-databases test-client)
  (._get-database test-client "testingone" "admin")

  (._create-database test-client {:db-id "university"
                                  :account-id "admin"
                                  :label "University Graph"
                                  :description "graph connect"})

  (._close test-client)
  )
