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

Defining Application Queries
============================

Let’s try the query-first approach to start designing the data model for
a hotel application. The user interface design for the application is
often a great artifact to use to begin identifying queries. Let’s assume
that you’ve talked with the project stakeholders and your UX designers
have produced user interface designs or wireframes for the key use
cases. You’ll likely have a list of shopping queries like the following:

-  Q1. Find hotels near a given point of interest.

-  Q2. Find information about a given hotel, such as its name and
   location.

-  Q3. Find points of interest near a given hotel.

-  Q4. Find an available room in a given date range.

-  Q5. Find the rate and amenities for a room.

It is often helpful to be able to refer
to queries by a shorthand number rather that explaining them in full.
The queries listed here are numbered Q1, Q2, and so on, which is how they
are referenced in diagrams throughout the example.

Now if the application is to be a success, you’ll certainly want
customers to be able to book reservations at hotels. This includes
steps such as selecting an available room and entering their guest
information. So clearly you will also need some queries that address the
reservation and guest entities from the conceptual data model. Even
here, however, you’ll want to think not only from the customer
perspective in terms of how the data is written, but also in terms of
how the data will be queried by downstream use cases.

You natural tendency as might be to focus first on
designing the tables to store reservation and guest records, and only
then start thinking about the queries that would access them. You may
have felt a similar tension already when discussing the
shopping queries before, thinking “but where did the hotel and point of
interest data come from?” Don’t worry, you will see soon enough.
Here are some queries that describe how users will access
reservations:

-  Q6. Lookup a reservation by confirmation number.

-  Q7. Lookup a reservation by hotel, date, and guest name.

-  Q8. Lookup all reservations by guest name.

-  Q9. View guest details.

All of the queries are shown in the context of the workflow of the
application in the figure below. Each box on the diagram represents a
step in the application workflow, with arrows indicating the flows
between steps and the associated query. If you’ve modeled the application
well, each step of the workflow accomplishes a task that “unlocks”
subsequent steps. For example, the “View hotels near POI” task helps
the application learn about several hotels, including their unique keys.
The key for a selected hotel may be used as part of Q2, in order to
obtain detailed description of the hotel. The act of booking a room
creates a reservation record that may be accessed by the guest and
hotel staff at a later time through various additional queries.

.. image:: images/data_modeling_hotel_queries.png

*Material adapted from Cassandra, The Definitive Guide. Published by
O'Reilly Media, Inc. Copyright © 2020 Jeff Carpenter, Eben Hewitt.
All rights reserved. Used with permission.*