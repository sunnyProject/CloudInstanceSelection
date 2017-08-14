function tw=tolexamine(utw,dw,CIC,RIC)
%AHP totally consistency check
tw=dw*utw;
CR=utw'*CIC/(utw'*RIC);
if CR>=0.10
    disp([input('The level of the total sort does not pass the consistency check, please re-adjust the judgment matrix')]);
else
    disp([input('The level of the total sort passes the consistency check')]);
end