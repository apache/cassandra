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

.. highlight:: cql

Defining Database Schema
========================

Once you have finished evaluating and refining the physical model, you’re
ready to implement the schema in CQL. Here is the schema for the
``hotel`` keyspace, using CQL’s comment feature to document the query
pattern supported by each table::

    CREATE KEYSPACE hotel WITH replication =
      {‘class’: ‘SimpleStrategy’, ‘replication_factor’ : 3};

    CREATE TYPE hotel.address (
      street text,
      city text,
      state_or_province text,
      postal_code text,
      country text );

    CREATE TABLE hotel.hotels_by_poi (
      poi_name text,
      hotel_id text,
      name text,
      phone text,
      address frozen<address>,
      PRIMARY KEY ((poi_name), hotel_id) )
      WITH comment = ‘Q1. Find hotels near given poi’
      AND CLUSTERING ORDER BY (hotel_id ASC) ;

    CREATE TABLE hotel.hotels (
      id text PRIMARY KEY,
      name text,
      phone text,
      address frozen<address>,
      pois set )
      WITH comment = ‘Q2. Find information about a hotel’;

    CREATE TABLE hotel.pois_by_hotel (
      poi_name text,
      hotel_id text,
      description text,
      PRIMARY KEY ((hotel_id), poi_name) )
      WITH comment = Q3. Find pois near a hotel’;

    CREATE TABLE hotel.available_rooms_by_hotel_date (
      hotel_id text,
      date date,
      room_number smallint,
      is_available boolean,
      PRIMARY KEY ((hotel_id), date, room_number) )
      WITH comment = ‘Q4. Find available rooms by hotel date’;

    CREATE TABLE hotel.amenities_by_room (
      hotel_id text,
      room_number smallint,
      amenity_name text,
      description text,
      PRIMARY KEY ((hotel_id, room_number), amenity_name) )
      WITH comment = ‘Q5. Find amenities for a room’;


Notice that the elements of the partition key are surrounded
with parentheses, even though the partition key consists
of the single column ``poi_name``. This is a best practice that makes
the selection of partition key more explicit to others reading your CQL.

Similarly, here is the schema for the ``reservation`` keyspace::

    CREATE KEYSPACE reservation WITH replication = {‘class’:
      ‘SimpleStrategy’, ‘replication_factor’ : 3};

    CREATE TYPE reservation.address (
      street text,
      city text,
      state_or_province text,
      postal_code text,
      country text );

    CREATE TABLE reservation.reservations_by_confirmation (
      confirm_number text,
      hotel_id text,
      start_date date,
      end_date date,
      room_number smallint,
      guest_id uuid,
      PRIMARY KEY (confirm_number) )
      WITH comment = ‘Q6. Find reservations by confirmation number’;

    CREATE TABLE reservation.reservations_by_hotel_date (
      hotel_id text,
      start_date date,
      end_date date,
      room_number smallint,
      confirm_number text,
      guest_id uuid,
      PRIMARY KEY ((hotel_id, start_date), room_number) )
      WITH comment = ‘Q7. Find reservations by hotel and date’;

    CREATE TABLE reservation.reservations_by_guest (
      guest_last_name text,
      hotel_id text,
      start_date date,
      end_date date,
      room_number smallint,
      confirm_number text,
      guest_id uuid,
      PRIMARY KEY ((guest_last_name), hotel_id) )
      WITH comment = ‘Q8. Find reservations by guest name’;

    CREATE TABLE reservation.guests (
      guest_id uuid PRIMARY KEY,
      first_name text,
      last_name text,
      title text,
      emails set,
      phone_numbers list,
      addresses map<text,
      frozen<address>,
      confirm_number text )
      WITH comment = ‘Q9. Find guest by ID’;

You now have a complete Cassandra schema for storing data for a hotel
application.

*Material adapted from Cassandra, The Definitive Guide. Published by
O'Reilly Media, Inc. Copyright © 2020 Jeff Carpenter, Eben Hewitt.
All rights reserved. Used with permission.*