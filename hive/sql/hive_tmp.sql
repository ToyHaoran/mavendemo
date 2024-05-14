select t1.id,t2.pkey from t1 join t2 on t1.pkey=t2.pkey and t1.p key in(select t2.pkey from t2 where t2.id<2)
