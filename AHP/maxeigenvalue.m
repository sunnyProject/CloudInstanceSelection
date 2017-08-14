%%To get the first eigenvalue and corresponding Normalized eigenvector, A is decision matrix
function[maxeigval,w]=maxeigenvalue(A)

[eigvec,eigval] = eig(A);
eigval = diag(eigval);
eigvalmag = imag(eigval);
realind = find(eigvalmag<eps);
realeigval = eigval(realind);
maxeigval = max(realeigval) %First eigenvalue
index=find(eigval==maxeigval);
vecinit = eigvec(:,index);%First eigenvector
w = vecinit./sum(vecinit)%Normalized eigenvector