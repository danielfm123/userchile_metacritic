select count(*)
from mc_indice
left join mc_juegos
on mc_indice.id = mc_juegos.id
where mc_juegos.id is null;

select count(distinct id), count(*) from mc_detail;
select * from mc_detail;
#truncate table mc_detail;
select * from mc_juegos where id = 203;

select * from mc_indice where  nombre like '%metroid%';

select count(*) from mc_indice where metascore <> 'tbd';

select id,count(*) from mc_detail group by id;
truncate mc_detail;

select count(*) from mc_indice where metascore <> 'tbd';

select * from mc_juegos where publisher is not null;
select sistema,pagina, count(*) from mc_indice group by pagina,sistema order by sistema,pagina;
select count(*) from mc_indice where metascore <> 'tbd';
