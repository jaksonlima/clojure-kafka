(ns clojure-kafka.core
  (:require [clj-kafka.producer :as producer]
            [clj-kafka.consumer.simple :as consumer]))

(def producer-config
  {"metadata.broker.list" "localhost:9094"
   "serializer.class" "kafka.serializer.StringEncoder"
   "request.required.acks" "1"})

(defn create-producer []
  (producer/producer producer-config))

(defn send-message [producer topic key value]
  (->>(producer/message topic key value)
     (producer/send-message producer)))

(defn create-consumer []
  (consumer/consumer "localhost" 9094 "example-client" :timeout 10000 :buffer-size 10000))

(defn consume-messages [consumer topic partition]
  (let [offset (- (consumer/topic-offset consumer topic partition :latest) 1)]
    (let [messages (consumer/messages consumer "example-client" topic partition offset 1000)]
        (doseq [message messages]
          (println "Received message:" (String. (.value message)) " Offset: " (.offset message))))))

(defn loop [callback]
  (while true
    (callback)
    (Thread/sleep 5000)))

(defn -main [& args]
  (let [producer (create-producer)
        consumer (create-consumer)]
    (send-message producer "clojure-kafka" "lima" "Hello, Kafka")
    (Thread/sleep 2000)
    (loop (fn [] (consume-messages consumer "clojure-kafka" 0)))
    (.close producer)
    (.close consumer)))

