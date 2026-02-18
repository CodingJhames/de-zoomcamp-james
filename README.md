# Data Engineering Zoomcamp - AWS Edition 🚀

This repository documents my journey through the **Data Engineering Zoomcamp**. Due to technical constraints with GCP, I pivoted the entire infrastructure to **Amazon Web Services (AWS)**, demonstrating adaptability and a deep understanding of cloud-agnostic engineering principles.

---

### 🛠️ Project Evolution & Milestones

#### Phase 1: Infrastructure as Code (Terraform)
Instead of manual configuration, I built the foundation using **Terraform** to ensure a reproducible environment:
* **Storage:** Provisioned **AWS S3** as the primary Data Lake (replacing GCS).
* **Automation:** Utilized `variables.tf` and `main.tf` to manage regional settings (`us-east-1`) and bucket policies without hardcoding values.

#### Phase 2: Batch Processing (Spark + EC2) — *Week 5*
I successfully processed large-scale NYC Taxi datasets while overcoming significant hardware constraints:
* **Compute:** Configured an **EC2 (Ubuntu 24.04 LTS)** instance.
* **Resource Optimization (The Swap Trick):** Since the `t3.micro` instance is limited to 1GB of RAM, I manually implemented **4GB of Swap Memory**. This "virtual RAM" allowed Spark to process heavy Parquet files without system crashes—proving that engineering logic can overcome hardware limitations.
* **Technical Stack:** Installed and tuned OpenJDK 11, PySpark 3.3.2, and Python 3.12 within a hardened Linux environment.



#### Phase 3: Orchestration & Data Quality (Bruin)
Integrated modern orchestration logic to manage data pipelines:
* **Materialization:** Applied incremental strategies (`time_interval`) to optimize processing costs.
* **Data Integrity:** Implemented **Quality Checks** (`not_null`) to ensure the reliability of the staging layer.
* **Lineage:** Utilized dependency graphs to visualize asset relationships.

---

### 📂 Repository Structure
* `/terraform`: Infrastructure configuration files (.tf).
* `solucion.py`: PySpark processing script for Week 5.
* `.gitignore`: Configured to protect sensitive files like AWS `.pem` keys and Terraform state.

---

### 🚀 How to Run
1. **Infrastructure:** Run `terraform init` followed by `terraform apply` to provision the S3 Data Lake.
2. **Server Setup:** Use the provided bash commands to configure Java and the **4GB Swapfile** on EC2.
3. **Data Processing:** Execute the Spark jobs using `python3 solucion.py`.

---

> **Engineering Philosophy:**
> I pivoted to AWS out of **practicality and resilience**. A data engineer’s value isn't tied to a specific vendor (Google or Amazon), but to the ability to build reliable bridges regardless of the terrain. Logic is universal; tools are temporary.