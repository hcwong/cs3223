SELECT *
FROM Flights,Schedule,Aircrafts,Certified,Employees
WHERE Flights.flno=Schedule.flno,Schedule.aid=Aircrafts.aid,Certified.aid=Aircrafts.aid,Certified.eid=Employees.eid
