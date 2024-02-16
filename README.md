This project aims to analyze large volumes of flight data efficiently to identify airport hubs, track airline activity, and sort flights by airline at various airports.

Storm Topology: Utilizes a Storm topology to process and analyze flight data. Comprises a spout and two bolts for data ingestion, airport identification, and airline sorting.

Components:
  FlightsDataReader (Spout):Reads flight data from a text file and emits it to the topology.
  HubIdentifier (Bolt): Identifies airports near each flight based on geographic coordinates.
  AirlineSorter (Bolt):Maps flight data to respective airports and maintains airline counts.

Data Handling:
  Airport and flight data are parsed from text files.
  Airport data includes name, code, latitude, and longitude.
  Flight data includes attributes such as latitude, longitude, and call sign.

Technologies Used:
  Storm: Provides a distributed computation system for processing large volumes of data streams efficiently.
  Java Programming Language
