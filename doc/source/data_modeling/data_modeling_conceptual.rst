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

.. conceptual_data_modeling

Conceptual Data Modeling
^^^^^^^^^^^^^^^^^^^^^^^^

First, let’s create a simple domain model that is easy to understand in
the relational world, and then see how you might map it from a relational
to a distributed hashtable model in Cassandra.

Let's use an example that is complex enough
to show the various data structures and design patterns, but not
something that will bog you down with details. Also, a domain that’s
familiar to everyone will allow you to concentrate on how to work with
Cassandra, not on what the application domain is all about.

For example, let's use a domain that is easily understood and that
everyone can relate to: making hotel reservations.

The conceptual domain includes hotels, guests that stay in the hotels, a
collection of rooms for each hotel, the rates and availability of those
rooms, and a record of reservations booked for guests. Hotels typically
also maintain a collection of “points of interest,” which are parks,
museums, shopping galleries, monuments, or other places near the hotel
that guests might want to visit during their stay. Both hotels and
points of interest need to maintain geolocation data so that they can be
found on maps for mashups, and to calculate distances.

The conceptual domain is depicted below using the entity–relationship
model popularized by Peter Chen. This simple diagram represents the
entities in the domain with rectangles, and attributes of those entities
with ovals. Attributes that represent unique identifiers for items are
underlined. Relationships between entities are represented as diamonds,
and the connectors between the relationship and each entity show the
multiplicity of the connection.

.. image:: images/data_modeling_hotel_erd.png

Obviously, in the real world, there would be many more considerations
and much more complexity. For example, hotel rates are notoriously
dynamic, and calculating them involves a wide array of factors. Here
you’re defining something complex enough to be interesting and touch on
the important points, but simple enough to maintain the focus on
learning Cassandra.

*Material adapted from Cassandra, The Definitive Guide. Published by
O'Reilly Media, Inc. Copyright © 2020 Jeff Carpenter, Eben Hewitt.
All rights reserved. Used with permission.*