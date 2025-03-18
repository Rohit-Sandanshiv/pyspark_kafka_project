# Pyspark-kafka Project
This project runs a PySpark job to process and transform account, party, and address data, then publishes the transformed data to Kafka.

## File Structure
```
├── app-logs/
│   ├── hello-spark.log
├── conf/
│   ├── sbdl.conf
│   ├── spark.conf
├── lib/
│   ├── __init__.py
│   ├── DataLoader.py
│   ├── kafka_stream.py
│   ├── logger.py
│   ├── transformation.py
│   ├── utils.py
├── test_data/
│   ├── accounts/
│   ├── parties/
│   ├── party_address/
│   ├── results/
│   │   ├── contract_df.json
│   │   ├── final_df.json
├── venv/  # Virtual environment
├── .gitignore
├── log4j.properties
├── Pipfile
├── practise.py
├── sbdl_main.py
├── sbdl_submit.sh
├── test_pytest_sbdl.py 
│── requirements.txt
```

---

## Installation & Setup
### Prerequisites
- Python 3.7+
- `pip` (Python package manager)
- Apache Spark 3.5.4
- Kafka
- Pipenv (for dependency management)

### Steps
 ## 1. Clone the repository:
   ```sh
   git clone https://github.com/Rohit-Sandanshiv/pyspark_kafka_project
   cd pyspark_kafka_project
   ```
## 2. Create and activate a virtual environment:
   ```sh
   python -m venv venv
   source venv/bin/activate  # On Mac/Linux
   venv\Scripts\activate  # On Windows
   ```
## 3. Install dependencies:
   ```sh
   pip install -r requirements.txt
   ```

## 4. configure kafka:
1) **Download and Extract Kafka**
- Download the latest Kafka package: kafka_2.13-3.9.0 from https://kafka.apache.org/downloads.
- Extract it in C:\kafka (for simplicity).
2) **in first terminal:(start zookeeper server)**
```
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
```

3) **in terminal2 (start kafka server)**
```
bin\windows\kafka-server-start.bat config\server.properties
```

4) **in terminal3 (create a producer and send example message)**
```
bin\windows\kafka-console-producer.bat --topic sbdl_topic --bootstrap-server localhost:9092
>testing1
>testing2
```
 This way we can send multiple messages

5) **in terminal4 (create a consumer which will consumer msg)**
```
bin\windows\kafka-console-consumer.bat --topic sbdl_topic --from-beginning --bootstrap-server localhost:9092
```
- testing1
- testing2
- if you see these, that means kafka is sending/receiving
---

## Description of sbdl_main.py
- **Initializes a Spark session**
- **Reads and transforms account, party, and address data.**
- **Joins data to create a complete dataset.**
- **Prepares the data and publishes it to Kafka.**

## Kafka Output

- **Data is published to Kafka under the topic sbdl_topic.**
- **Each message contains transformed data in JSON format.**

## Logging
- **Logs are stored in app-logs/hello-spark.log.**
- **Uses a custom Log4j class for logging Spark jobs.**

## Testing
- **Run unit tests using pytest:**
```
pytest test_pytest_sbdl.py
```
## Future Scope & Improvements
1. **Optimize Spark performance with caching and partitioning.**
2. **Enhance error handling and logging mechanisms.**
3. **Implement data encryption and compliance measures.**
4. **Automate deployments with CI/CD pipelines.**
5. **Extend support for additional data sources.**


---


## Contribution
Feel free to contribute by submitting issues or pull requests.

---

## Author
**Rohit Sandanshiv**

---

## License
MIT License

