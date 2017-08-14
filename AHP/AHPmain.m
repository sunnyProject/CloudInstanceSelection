%% cpu-2 ram-3.75 storage-20
clear all;
%% construct data axis
%1-Equal Importance 3-Moderate importance 5-Strong importance 
%7-Very strong or demonstrated importance 9-Extreme importance
A = [1,1,3;1,1,3;1/3,1/3,1];
B1 = [1,1/3,1,1/3,1/3,1;
     3,1,3,1,1,2;
     1,1/3,1,1/3,1/3,1;
     3,1,3,1,1,2;
     3,1,3,1,1,2;
     2,1/2,1,1/2,1/2,1];
B2 = [1,1/2,1/3,3,1/3,3;
     2,1,1/2,4,1/2,4;
     3,2,1,5,1,5;
     1/3,1/4,1/5,1,1/5,1;
     3,2,1,5,1,5;
     1/3,1/4,1/5,1,1/5,1];
B3 = [1,1,5,3,1,1;
     1,1,5,3,1,1;
     1/5,1/5,1,1/3,1/5,1/5;
     1/3,1/3,3,1,1/3,1/3;
     1,1,5,3,1,1;
     1,1,5,3,1,1;];
%%
[max1(1),wA]=maxeigenvalue(A);
[max1(2),wB1]=maxeigenvalue(B1);
[max1(3),wB2]=maxeigenvalue(B2);
[max1(4),wB3]=maxeigenvalue(B3);
%[max1(5),wB4]=maxeigenvalue(B4);
%%
[RIA,CIA]=examine(max1(1),A);
[RIB1,CIB1]=examine(max1(2),B1);
[RIB2,CIB2]=examine(max1(3),B2);
[RIB3,CIB3]=examine(max1(4),B3);
%[RIB4,CIB4]=examine(max1(5),B4);
CIC=[CIB1;CIB2;CIB3];%CIB4;
RIC=[RIB1;RIB2;RIB3];%RIB4;
%%
dw=zeros(6,3);
dw(:,1)=wB1;
dw(:,2)=wB2;
dw(:,3)=wB3;
%dw(:,4)=wB4;

%%
tw = tolexamine(wA,dw,CIC,RIC)';
[MAX,CHOICE] = max(tw);
CHOICE
