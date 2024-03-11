-- 登录Oracle数据库：通过sqlplus 数据库用户lihaoran 密码lihaoran；
sqlplus lihaoran/lihaoran@orcl
show user --显示当前用户
conn lisi/lisi@orcl --切换用户
-- 若以sys登录，必须指明连接身份(默认normal)，否则报错；sysdba数据库管理员 sysoper数据库操作员
conn sys/sys @orcl as sysdba;
passw lihaoran --更改密码 以高级别用户重置低级别用户的密码 或 知道旧密码
alter user sys identified by sys;

-- 创建用户 必须拥有DBA权限 新用户需要权限才可登录DB，默认表空间为users，临时表空间为temp。
create user lisi identified by 12345; -- 密码不要以数字开头
drop user lisi cascade; -- 删除用户(需要DBA权限) (cascade级联删除)
-- 赋权限 收回权限；角色是权限的集合，避免权限授予混乱。
-- DBA：系统最高权限，可创建数据库结构，创建用户，给其他用户授权，收回权限等全部权限。
-- resource：可创建表、视图、索引等，不可创建数据库结构。
-- connect: 会话权限，只可登录oracle，不可创建实体，不可建库结构
create role myrole;
grant create session,create table to myrole; --session 登录数据库的权限
grant myrole,resource to lisi; -- 授予权限或角色给lisi
select * from user_sys_privs; -- 查看用户user拥有的权限
select * from user_role_privs; -- 查看用户user拥有的角色
revoke create table from lisi; -- 收回用户的权限 需要DBA权限
drop role myrole;