.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

Physical Data Modeling
======================

Once you have a logical data model defined, creating the physical model
is a relatively simple process.

You walk through each of the logical model tables, assigning types to
each item. You can use any valid :ref:`CQL data type <data-types>`,
including the basic types, collections, and user-defined types. You may
identify additional user-defined types that can be created to simplify
your design.

After you’ve assigned data types, you analyze the model by performing
size calculations and testing out how the model works. You may make some
adjustments based on your findings. Once again let's cover the data
modeling process in more detail by working through an example.

Before getting started, let’s look at a few additions to the Chebotko
notation for physical data models. To draw physical models, you need to
be able to add the typing information for each column. This figure
shows the addition of a type for each column in a sample table.

.. image:: images/data_modeling_chebotko_physical.png

The figure includes a designation of the keyspace containing each table
and visual cues for columns represented using collections and
user-defined types. Note the designation of static columns and
secondary index columns. There is no restriction on assigning these as
part of a logical model, but they are typically more of a physical data
modeling concern.

Hotel Physical Data Model
-------------------------

Now let’s get to work on the physical model. First, you need keyspaces
to contain the tables. To keep the design relatively simple, create a
``hotel`` keyspace to contain tables for hotel and availability
data, and a ``reservation`` keyspace to contain tables for reservation
and guest data. In a real system, you might divide the tables across even
more keyspaces in order to separate concerns.

For the ``hotels`` table, use Cassandra’s ``text`` type to
represent the hotel’s ``id``. For the address, create an
``address`` user defined type. Use the ``text`` type to represent the
phone number, as there is considerable variance in the formatting of
numbers between countries.

While it would make sense to use the ``uuid`` type for attributes such
as the ``hotel_id``, this document uses mostly ``text`` attributes as
identifiers, to keep the samples simple and readable. For example, a
common convention in the hospitality industry is to reference properties
by short codes like "AZ123" or "NY229". This example uses these values
for ``hotel_ids``, while acknowledging they are not necessarily globally
unique.

You’ll find that it’s often helpful to use unique IDs to uniquely
reference elements, and to use these ``uuids`` as references in tables
representing other entities. This helps to minimize coupling between
different entity types. This may prove especially effective if you are
using a microservice architectural style for your application, in which
there are separate services responsible for each entity type.

As you work to create physical representations of various tables in the
logical hotel data model, you use the same approach. The resulting design
is shown in this figure:

.. image:: images/data_modeling_hotel_physical.png

Note that the ``address`` type is also included in the design. It
is designated with an asterisk to denote that it is a user-defined type,
and has no primary key columns identified. This type is used in
the ``hotels`` and ``hotels_by_poi`` tables.

User-defined types are frequently used to help reduce duplication of
non-primary key columns, as was done with the ``address``
user-defined type. This can reduce complexity in the design.

Remember that the scope of a UDT is the keyspace in which it is defined.
To use ``address`` in the ``reservation`` keyspace defined below
design, you’ll have to declare it again. This is just one of the many
trade-offs you have to make in data model design.

Reservation Physical Data Model
-------------------------------

Now, let’s examine reservation tables in the design.
Remember that the logical model contained three denormalized tables to
support queries for reservations by confirmation number, guest, and
hotel and date. For the first iteration of your physical data model
design, assume you're going to manage this denormalization
manually. Note that this design could be revised to use Cassandra’s
(experimental) materialized view feature.

.. image:: images/data_modeling_reservation_physical.png

Note that the ``address`` type is reproduced in this keyspace and
``guest_id`` is modeled as a ``uuid`` type in all of the tables.

*Material adapted from Cassandra, The Definitive Guide. Published by
O'Reilly Media, Inc. Copyright © 2020 Jeff Carpenter, Eben Hewitt.
All rights reserved. Used with permission.*