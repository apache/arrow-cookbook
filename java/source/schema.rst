===================
Working with schema
===================

Common definition of table has an schema. Java arrow is columnar oriented and it also has an schema representation. 
Consider that each name on the schema maps to a columns for a predefined data type


.. contents::

Define data type
================

Definition of columnar fields for string (name), integer (age) and array (points):

.. literalinclude:: demo/src/main/java/Definition.java
   :lines: 21-22, 30-37
   :language: java


Define metadata
===============

.. literalinclude:: demo/src/main/java/Definition.java
   :lines: 24-28
   :language: java

.. code-block::

    {A=Id card, B=Passport, C=Visa}

Create the schema
=================

Tables detain multiple columns, each with its own name
and type. The union of types and names is what defines a schema.

A schema in Arrow can be defined using :meth:`org.apache.arrow.vector.types.pojo.Schema`

.. literalinclude:: demo/src/main/java/Definition.java
   :lines: 40
   :language: java

Populate data
=============

.. literalinclude:: demo/src/main/java/Definition.java
   :lines: 45-55
   :language: java

Create the schema from json
===========================

For this json definition:

.. code-block::

    {
      "fields" : [ {
        "name" : "name",
        "nullable" : true,
        "type" : {
          "name" : "utf8"
        },
        "children" : [ ]
      }, {
        "name" : "document",
        "nullable" : true,
        "type" : {
          "name" : "utf8"
        },
        "children" : [ ],
        "metadata" : [ {
          "value" : "Id card",
          "key" : "A"
        }, {
          "value" : "Passport",
          "key" : "B"
        }, {
          "value" : "Visa",
          "key" : "C"
        } ]
      }, {
        "name" : "age",
        "nullable" : true,
        "type" : {
          "name" : "int",
          "bitWidth" : 32,
          "isSigned" : true
        },
        "children" : [ ]
      }, {
        "name" : "points",
        "nullable" : true,
        "type" : {
          "name" : "list"
        },
        "children" : [ {
          "name" : "intCol",
          "nullable" : true,
          "type" : {
            "name" : "int",
            "bitWidth" : 32,
            "isSigned" : true
          },
          "children" : [ ]
        } ]
      } ]
    }

Java arrow offer Schema.fromJSON() method to create an schema from json definition

.. literalinclude:: demo/src/main/java/Definition.java
   :lines: 69-70
   :language: java

.. code-block::

   Schema<name: Utf8, document: Utf8, age: Int(32, true), points: List<intCol: Int(32, true)>>

Code
====

.. literalinclude:: demo/src/main/java/Definition.java
   :language: java

Output:

.. code-block::

    name    document    age points
    david   A   10  [1,3,5,7,9]
    gladis  B   20  [2,4,6,8,10]
    juan    C   30  [1,2,3,5,8]


