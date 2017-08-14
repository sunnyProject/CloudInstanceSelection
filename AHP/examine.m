function[RI,CI]=examine(maxeigval,A)
%AHP consistency check
%maxeigval is fist eigenvalue, A is decision matrix
n=size(A,1);
RIT=[0,0,0.58,0.90,1.12,1.24,1.32,1.41,1.45,1.49,1.51];
RI=RIT(n);
CI=(maxeigval-n)/(n-1);
CR=CI/RI;
if CR>=0.10
    disp([input('The matrix does not pass the consistency check, please readjust the matrix')])
else
    disp([input('The matrix passes the consistency check')]);
end