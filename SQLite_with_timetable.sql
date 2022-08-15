/*drop table IF EXISTS  t; 

CREATE TABLE t (id int, timestam int, status text);
INSERT into t VALUES
(1, 4, 'a')
,(1, 6, 'b')
,(1, 17, 'с')
,(1, 20, 'd')
,(2, 5, 'a')
,(2, 6, 'b')
,(2, 7, 'с');*/


 
drop table IF EXISTS  result;
drop table IF EXISTS intermediate_result;
drop table IF EXISTS  tab; 

CREATE  TABLE tab (id int, timestam int, status text);
CREATE  TABLE result (status text, Time_in_minutes decimal(10,1));
CREATE  TABLE intermediate_result(status text, Time_in_minutes decimal(10,1), id int);


INSERT into tab 
select distinct * from deal
order by id, timestamp;



INSERT into intermediate_result(time_in_minutes, status, id)     --ЗАПОЛНЯЕМ ПРОМЕЖУТОЧНУЮ ТАБЛИЦУ С ПОЛЯМИ : ДЛИТЕЛЬНОСТЬ СТАТУСТА (time_in_minutes), СТАТУС, ID ЗАКАЗА
select case when id = LEAD(id,1,0) OVER (
                  PARTITION BY id 
                  ORDER BY timestam asc
                  ) 
            then (LEAD(timestam,1,0) OVER (               --ЕСЛИ ID СЛЕДУЮЩЕГО ЗАПРОСА И ТЕКУЩЕГО СОВПАДАЮТ, ТО СЧИТАЕМ ДЛИТЕЛЬНОСТЬ ТЕКУЩЕГО В МИНУТАХ, ИНАЧЕ NULL
                    PARTITION BY id 
                    ORDER BY timestam asc
                  ) - timestam) / 60.0 
      end,
        status,
        id
    end
from tab;
 

 
--SELECT * from intermediate_result;
  
INSERT into result(time_in_minutes, status)
select round(avg(time_in_minutes), 1), status
from intermediate_result
where time_in_minutes is not null               --ЕСЛИ В TIME IN MINUTES СТОИТ NULL, ЗНАЧИТ ЭТО ПОСЛЕДНИЙ СТАТУС КОНКРЕТНОГО ЗАКАЗА
GROUP BY status;

SELECT * from result;			 --ВЫВОД ТАБЛИЦЫ СО СРЕДНЕЙ ДЛИТЕЛЬНОСТЬЮ СТАТУСОВ



select id, status from intermediate_result   --ВЫВОД ТАБЛИЦЫ С КОНЕЧНЫМИ/ТЕКУЩИМИ СТАТУСАМИ
where time_in_minutes is null 
order by id;



 --НИЖЕ ПРОИСХОДИТ ВЫВОД ТАБЛИЦЫ С ЗАКОНЧИВШИМСЯ СТАТУСАМИ "Проверка товара на складе" : id, нормированное время открытия в часах, время закрытия в часах, длительность в часах
                      
select id, round((timestam / 3600.0)- 460242, 2) as start_time , round((end_timestamp / 3600.0)  - 460242, 2) as end_time, round((end_timestamp - timestam) / 3600.0, 2) as 'status_duration (hours)' from (
select id, status, timestam, case when id = LEAD(id,1,0) OVER (
                      PARTITION BY id 
                      ORDER BY timestam asc
                      )
                      then 
                      LEAD(timestam,1,0) OVER (
                      PARTITION BY id 
                      ORDER BY timestam asc
                      ) end as end_timestamp

from tab)
where end_timestamp is not null and timestam != end_timestamp and status = 'Проверка товара на складе'
ORDER by end_time

/*select COUNT(*) from (
  select *, (end_timestamp - timestam) / 360.0 from (
select id, status, timestam, case when id = LEAD(id,1,0) OVER (
                      PARTITION BY id 
                      ORDER BY timestam asc
                      )
                      then 
                      LEAD(timestam,1,0) OVER (
                      PARTITION BY id 
                      ORDER BY timestam asc
                      ) end as end_timestamp

from tab)
where end_timestamp is not null and timestam != end_timestamp and status = 'Проверка товара на складе')*/



/*select sum(time_in_minutes) as sum_time, round(avg(time_in_minutes), 1) as average_duration, status, count(*) as status_count
from intermediate_result
where time_in_minutes is not null              
GROUP BY status;*/


                      