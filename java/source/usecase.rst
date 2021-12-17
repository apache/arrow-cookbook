========
Use Case
========

Code to solve common use case tru apache java arrow


.. contents::


Use case - Processing data filter & aggregation
===============================================

Scenario: Read twitter post for processing

Question: What is the average age per city that are talking about cryptocurrency for people between 21-27 years on twitter post

Data input:

.. code-block::

    name        topic            city           age
    david       cryptocurrency   lima           21
    gladis      fashion          cuzco          22
    juan        cryptocurrency   huancayo       35
    pedro       healthcare       tarapoto       23
    oscar       security         lima           27
    ronald      cryptocurrency   lima           44
    francisco   cryptocurrency   lima           25

Code
====

.. literalinclude:: demo/src/main/java/usecase/ProcessingData.java
   :language: java

.. code-block::

    Age filter index: [0, 1, 2, 3, 4, 6]
    City filter index: [0, 2, 5, 6]
    Cross-filter-index: [0, 2, 6]
    City-count-aggregation: {lima=2, huancayo=1}
    City-sum-age-aggregation: {lima=46, huancayo=26}
    >>>>> REPORT <<<<< 
    City: lima, Number of person: 2, Age average talking about criptocurrency: 23.0
    City: huancayo, Number of person: 1, Age average talking about criptocurrency: 26.0