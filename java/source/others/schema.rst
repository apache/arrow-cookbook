===================
Working with Schema
===================

Common definition of table has an schema. Java arrow is columnar oriented and it also has an schema representation.
Consider that each name on the schema maps to a columns for a predefined data type


.. contents::

Define Data Type
================

Definition of columnar fields for string (name), integer (age) and array (points):

.. testcode::

    import org.apache.arrow.vector.types.pojo.ArrowType;
    import org.apache.arrow.vector.types.pojo.Field;
    import org.apache.arrow.vector.types.pojo.FieldType;

    Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
    System.out.print(name)

.. testoutput::

    name: Utf8