# DIRT – Semantic Similarity of Paths with Hadoop MapReduce on AWS EMR

An implementation of the **DIRT algorithm** (*Discovery of Inference Rules from Text*, Lin & Pantel) in **Java** with **Hadoop MapReduce**, running on **AWS EMR**. The system learns which dependency paths have similar meanings, for example *"X solves Y"* ≈ *"X finds a solution to Y"*, by analyzing a large corpus of syntactic n-grams.

**Tech:** Java · Hadoop MapReduce · AWS EMR · S3 · Snowball Stemmer · AWS SDK for Java · Maven

---

## Pipeline (chained MapReduce jobs)

1. **Path extraction** – parses syntactic n-grams, extracts `X <verb> [prep] Y` dependency paths together with their slot fillers, and normalizes words with the **Snowball stemmer** (auxiliary verbs are filtered out).
2. **Mutual Information** – computes the MI of each (path, slot, word) triple.
3. **Path strength** – aggregates the MI values for each path and slot.
4. **Similarity** – computes the Lin similarity between path pairs from the shared slot fillers.
5. **Evaluation** – scores a labeled test set of positive and negative predicate pairs.

## Scale
- Ran on 100 input files of the syntactic n-gram corpus.
- EMR cluster of 3 × `m5.xlarge` instances, launched from code (`App.java`).
- Input, output and logs are all stored on S3.

## Build & run
```bash
cd DIRT-MapReduce-Assignment
mvn clean package
# upload the JAR, the input and the test sets to S3, then:
export AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...  AWS_SESSION_TOKEN=...
java -cp target/<jar-name>.jar org.example.App
```

> Credentials are read from environment variables only. Nothing is stored in code.

---
*Team project – Distributed System Programming course, Ben-Gurion University. Developed with [maortab](https://github.com/maortab).*
