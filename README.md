# final-project
Premise

Trainees will be required to put together an ETL Pipeline for mock data from a fictionalised version of Sparta Global. Candidates for the academy have their contact details collected and then are invited to a particular Sparta Day. During the SpartaDay, each candidate’s performance on their psychometric tests and presentation are recorded. Each candidate is interviewed, and notes are made about their strengths, weaknesses, technologies known, etc. Their final result (pass / fail) and course interestis also noted.Assuming they pass the interview, candidates may end up attending the Sparta Academy. Each week, scores out of 8 are recorded for their performance on the 6 behaviourss used to assess trainees. Candidates with low scores may be removed from the academy.

Task

1. Design an appropriate store (e.g. a fully normalised database) to house all relevant data that can be drawn from the files provided. The database should provide a Single Person View.

2. Write production-level code to extract, transform and load the data into the database. Cleaned data may be pushed back to S3, but the raw data should be left untouched on the cloud. It should be possible to add new data to the S3 bucket, and have this picked up and incorporated by the pipeline.

3. Analyse the data to answer key business questions. If time permits, produce a dashboard to help with this analysis.

4. The trainees should work as a Scrum Team. Trainers are encouraged to assign roles to each trainee (Scrum Master, BA, Dev, Test) to give them opportunity to strengthen areas of weakness. The Trainer will act as the Product Owner. A sprint should be the length of the working day, and teams should utilise the Scrum Ceremonies: Sprint Planning, Frequent Scrum Meetings (or Stand-Ups), Sprint Reviews and Sprint Retrospectives.

5. The trainees should use a Jira Board in order to keep track of User Stories -set up one on the Sparta Global account for them to use, and give the Scrum Master admin rights.

6. The code written should be housed in a GitHub repository, with an appropriate Git workflow (e.g. Git Feature Branch Workflow)

7. On the final day of their course, the trainees will give a 30-60 minute presentation to present their solution.



Data
The data are to be provided to the students in an S3 bucket within AWS. The contents of data-engineering-project-master should be copied to a class-specific bucket during the Cloud module. The data are grouped into the following collections:
	•	‘Talent’ contains:
	◦	one CSV file per month of 2019, with information on candidates invited to a Sparta Assessment Day
	◦	one TXT file per Sparta Day, with the date and location of the event followed by a list of candidates along with their psychometric and presentation scores.
	◦	one JSON file per candidate, with notes on their answers to key interview questions, and a final pass/fail decision.
	•	‘Academy’ contains one CSV file per course, with data on trainees’ behavioural competency scores.
