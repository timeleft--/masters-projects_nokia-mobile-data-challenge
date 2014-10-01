masters-projects_nokia-mobile-data-challenge
============================================

As a Master's student, I participated in the Nokia Mobile Data Challenge 2012 (https://research.nokia.com/page/12000) and made a submission to the "Semantic Place Label Prediction" task (http://research.nokia.com/page/12353). The paper for the submission is available on my UWaterloo page (http://plg.uwaterloo.ca/~yaboulna/cs886_final.pdf), and it outlines my final approach. This code is all the code I wrotee during that project. I was quickly hacking together driver programs for running different Machine Learning libraries with implementations for the algorithms I wanted to try. 

This was my first Machine Learning project, and I have learnt a lot from all the mess I made. You can see that I was using pretty low level paralellization primitives, and I wasn't using any frameworks to facilitate tracking running such jobs... I even wrote my own notifier to let me know when a job is done. Later on, I "discovered" Hadoop, and learnt that hacking ingress and egress code in Python is much easier than doing it in Java. 
