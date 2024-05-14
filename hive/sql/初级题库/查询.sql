select * from student_info;
select * from teacher_info;
select * from course_info;
select * from score_info;

-- 1 查询同姓（假设每个学生姓名的第一个字为姓）的学生名单并统计同姓人数大于2的姓
-- 先提取出每个学生的姓并分组，如果分组的count>=2则为同姓

select
    t1.first_name,
    count(*) count_first_name
from (
         select
             stu_id,
             stu_name,
             substr(stu_name,1,1) first_name
         from student_info
     ) t1
group by t1.first_name
having count_first_name >= 2;

-- 2 按照如下格式显示学生的语文、数学、英语三科成绩，没有成绩的输出为0，按照学生的有效平均成绩降序显示
-- 学生id 语文 数学 英语 有效课程数 有效平均成绩

select
    si.stu_id,
    sum(if(ci.course_name='语文',score,0)) `语文`,
    sum(if(ci.course_name='数学',score,0))  `数学`,
    sum(if(ci.course_name='英语',score,0))  `英语`,
    count(*)  `有效课程数`,
    avg(si.score)  `平均成绩`
from score_info si
    join course_info ci on si.course_id=ci.course_id
group by si.stu_id
order by `平均成绩` desc;

-- 3 查询一共参加三门课程且其中一门为语文课程(01)的学生的id和姓名
-- 先把没有语文课程的学生过滤掉，然后再聚合。

select
    t2.stu_id,
    s.stu_name
from (
         select t1.stu_id
         from (
                  select stu_id,
                         course_id
                  from score_info
                  where stu_id in (
                      select stu_id
                      from score_info
                      where course_id = "01"
                  )
              ) t1
         group by t1.stu_id
         having count(t1.course_id) = 3
     ) t2
         join student_info s on t2.stu_id = s.stu_id;

-- 4 查询所有课程成绩均小于60分的学生的学号、姓名
-- 使用子查询，增加列的方式
select s.stu_id,
       s.stu_name
from (
         select stu_id,
                sum(if(score >= 60, 1, 0)) flag
         from score_info
         group by stu_id
         having flag = 0
     ) t1
         join student_info s on s.stu_id = t1.stu_id;

-- 先把大于60分的stu_id记录下来，然后剩下的就是小于60分的。错误之处在于有人没考试。
select stu_id, stu_name
from student_info
where stu_id not in
    (select distinct stu_id
     from score_info
     where score>=60
     );


-- 查询学过“李体音”老师所教的所有课的同学的学号、姓名 (最好不要嵌套多层子查询)
select
    t1.stu_id,
    si.stu_name
from
    (select stu_id
     from score_info si
     where course_id not in
           (select course_id from course_info c
                 join teacher_info t on c.tea_id = t.tea_id
             where tea_name='李体音')
     group by stu_id
    ) t1
        left join student_info si on t1.stu_id=si.stu_id;

