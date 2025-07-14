# Airflow_Job_Monitoring

Problem Statement:

Managing large-scale workflows across data pipelines and automated processes is challenging due to limited real-time monitoring and alerting capabilities in traditional tools. This project addresses the need for a robust dashboard that:

•	Provides real-time insights into workflow execution.
•	Alerts users to task failures and delays automatically.
•	Enables data-driven optimizations based on analytics and performance metrics.

Background of the Problem Statement

Apache Airflow is widely used for workflow automation and orchestration but lacks features for comprehensive real-time monitoring and customizable alerts. Users often face delays in identifying and troubleshooting workflow issues. This project addresses these gaps by developing a monitoring dashboard that enhances visibility, facilitates quick responses, and supports efficient workflow management through automated notifications and actionable insights.


 Background & Motivation:
Apache Airflow is widely used for orchestrating workflows and managing complex data pipelines. However, its default UI lacks advanced real-time monitoring, alerting, and user-friendly dashboards that are essential for production-grade environments. This project was taken up as part of a case study with MassMutual India to address these gaps by designing and developing a custom monitoring dashboard that offers improved visibility, actionable insights, and timely alerts for Airflow workflows.

🎯 Objectives:
To build a real-time monitoring system for Airflow DAGs and task statuses.

To design an intuitive, user-friendly dashboard for easy navigation and workflow visibility.

To integrate automated alerting mechanisms (e.g., email notifications) for task failures or delays.

To ensure the solution is scalable and production-ready.

🛠️ Technology Stack:
Frontend: HTML, CSS, JavaScript (for dynamic and responsive UI)

Backend: Flask (to handle API requests and data communication)

Database: PostgreSQL (to store Airflow metadata, logs, and task history)

Integration: Apache Airflow API, EmailOperator

Hosting: Tested for local deployment and later prepared for production-level deployment.

⚙️ Implementation Details:
1. Backend Development
Used Flask to build a REST API that communicates with Airflow’s metadata database.

Queried task and DAG status from PostgreSQL, processed them, and served JSON data to the frontend.

2. Frontend Development
Designed a clean and interactive interface using HTML, CSS, and JavaScript.

Developed pages to display:

Dashboard View: Overall status of DAGs

Table View: Task-level logs

Statistics View: Task execution trends, pie charts, and bar graphs

Alerts Page: Shows failed tasks

Reschedule Page: Interface to manually or automatically reschedule tasks

3. Alert System
Integrated Airflow’s EmailOperator to send real-time email notifications in case of task failure or long delays.

Ensured that alerts are detailed and help engineers take quick action.

4. Testing & Deployment
The complete system was tested locally with various DAGs.

Demonstrated the working system at MassMutual India, where it was reviewed and appreciated by industry experts.

🎓 Learning Outcome & Skills Gained:
In-depth understanding of Airflow DAG orchestration, task scheduling, and metadata handling.

Hands-on experience in full-stack web development, API design, and database integration.

Improved skills in team collaboration, debugging, and real-time problem-solving.

Real-world exposure to enterprise-level workflow management requirements.

✅ Final Outcome:
The project successfully delivered a functional, real-time monitoring dashboard that bridged the limitations of Airflow’s default UI. The solution allowed:

Better visibility into task states

Quicker troubleshooting through automated alerts

Insights into system performance using analytics
     


<img width="365" height="323" alt="image" src="https://github.com/user-attachments/assets/9e7db8e9-0de1-4c97-a631-a6471846134c" />

<img width="374" height="329" alt="image" src="https://github.com/user-attachments/assets/bf16a13b-7a11-49cc-a1a2-6f095bdcb83e" />

<img width="374" height="349" alt="image" src="https://github.com/user-attachments/assets/8f6f8449-1f7a-4210-bb28-e41b0c1f86e3" />

<img width="380" height="356" alt="image" src="https://github.com/user-attachments/assets/671b38df-934b-4029-8941-a16871531097" />

<img width="377" height="357" alt="image" src="https://github.com/user-attachments/assets/1870029b-03a0-4da2-ad33-ab4e7a62adad" />




