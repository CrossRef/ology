; (ns ology.middleware.json
;   (:use ring.util.response))

; (defn- csv-request? [request]
;   (if-let [type (:content-type request)]
;     (not (empty? (re-find #"^text/csv" type)))))

; (defn- read-json [request & [keywords?]]
;   (if (json-request? request)
;     (if-let [body (:body request)]
;       (json/parse-string (slurp body) keywords?))))

; (defn wrap-json-body
;   "Middleware that parses the :body of JSON requests into a Clojure data
;   structure."
;   [handler & [{:keys [keywords?]}]]
;   (fn [request]
;     (if-let [json (read-json request keywords?)]
;       (handler (assoc request :body json))
;       (handler request))))

; (defn wrap-json-params
;   "Middleware that converts request bodies in JSON format to a map of
;   parameters, which is added to the request map on the :json-params and
;   :params keys."
;   [handler]
;   (fn [request]
;     (let [json (read-json request)]
;       (if (and json (map? json))
;         (handler (-> request
;                      (assoc :json-params json)
;                      (update-in [:params] merge json)))
;         (handler request)))))

; (defn wrap-csv-response
;   "Middleware that converts responses with a map or a vector for a body into a
;   CSV response."
;   [handler]
;   (fn [request]
;     (let [response (handler request)]
;       (if (coll? (:body response))
;         (let [csv-response (update-in response [:body] json/generate-string options)]
;           (if (contains? (:headers response) "Content-Type")
;             json-response
;             (content-type json-response "application/json; charset=utf-8")))
;         response))))