from cuml import AgglomerativeClustering as AC
import cupy as cp

#Create single linkage cluster using the Eulclidean metric
def cluster(D):
    model=AC(affinity='l2', connectivity='pairwise', linkage='single', n_clusters=1, output_type = 'cupy')
    model.fit(D);
    return model.children_



from sklearn.cluster import AgglomerativeClustering as AC_CPU
def cluster_CPU(D):
    #Create single linkage cluster
    model=AC_CPU(affinity='l2', linkage = 'single', n_clusters=1)
    model.fit(D);
    return model.children_



def seriation(Z):
    N = Z.shape[1]
    stack = [2*N-2]
    res_order = []
    while(len(stack) != 0):
        cur_idx = stack.pop()
        if cur_idx < N:
            res_order.append(cur_idx)
        else:
            stack.append(int(Z[1, cur_idx-N]))
            stack.append(int(Z[0, cur_idx-N]))
    return res_order


def seriation_CPU(Z):
    N=Z.shape[0]+1
    stack=[2*N-2]
    res_order=[]
    while(len(stack) != 0):
        cur_idx = stack.pop()
        if cur_idx < N:
            res_order.append(cur_idx)
        else:
            stack.append(int(Z[cur_idx-N, 1]))
            stack.append(int(Z[cur_idx-N, 0]))
    return res_order


def recursiveBisection(V, l, r, W):
    #Performs recursive bisection weighting for a new portfolio
    
    #V is the sorted correlation matrix
    #l is the left index of the recursion
    #r is the right index of the recursion
    #W is the list of weights
    
    if r-l == 1: #One item
        return W
    else:
        #Split up V matrix
        mid = l+(r-l)//2
        V1 = V[l:mid, l:mid]
        V2 = V[mid:r, mid:r]
        
        #Find new adjusted V
        V1_diag_inv = 1/cp.diag(V1)
        V2_diag_inv = 1/cp.diag(V2)
        w1 = V1_diag_inv/V1_diag_inv.sum()
        w2 = V2_diag_inv/V2_diag_inv.sum()
        V1_adj = w1.T@V1@w1
        V2_adj = w2.T@V2@w2
        
        #Adjust weights
        a2 = V1_adj/(V1_adj+V2_adj)
        a1 = 1-a2
        W[l:mid] = W[l:mid]*a1
        W[mid:r] = W[mid:r]*a2
        W = recursiveBisection(V, l, mid, W)
        W = recursiveBisection(V, mid, r, W)
        return W


def recursiveBisection_CPU(V, l, r, W):
    #Performs recursive bisection weighting for a new portfolio
    
    #V is the sorted correlation matrix
    #l is the left index of the recursion
    #r is the right index of the recursion
    #W is the list of weights
    
    if r-l == 1: #One item
        return W
    else:
        #Split up V matrix
        mid = l+(r-l)//2
        V1 = V[l:mid, l:mid]
        V2 = V[mid:r, mid:r]
        
        #Find new adjusted V
        V1_diag_inv = 1/np.diag(V1)
        V2_diag_inv = 1/np.diag(V2)
        w1 = V1_diag_inv/V1_diag_inv.sum()
        w2 = V2_diag_inv/V2_diag_inv.sum()
        V1_adj = w1.T@V1@w1
        V2_adj = w2.T@V2@w2
        
        #Adjust weights
        a2 = V1_adj/(V1_adj+V2_adj)
        a1 = 1-a2
        W[l:mid] = W[l:mid]*a1
        W[mid:r] = W[mid:r]*a2
        W = recursiveBisection_CPU(V, l, mid, W)
        W = recursiveBisection_CPU(V, mid, r, W)
        return W

# Define a function to perform HRP.
def HRP_GPU(cov, corr):
    nAssets = cov.shape[0]
    D = cp.sqrt(0.5*(1-corr))
    Z = cluster(D)
    res_order = seriation(Z)
    W_sort = recursiveBisection(cov[res_order, :][:, res_order], 0, nAssets, cp.ones(nAssets))
    W_ret = cp.empty(nAssets)
    W_ret[res_order] = W_sort
    return W_ret

import numpy as np
def HRP_CPU(cov, corr):
    cov = cov.get()
    corr = corr.get()
    nAssets = cov.shape[0]
    D = np.sqrt(0.5*(1-corr))
    Z = cluster_CPU(D)
    res_order = seriation_CPU(Z)
    W_sort = recursiveBisection_CPU(cov[res_order, :][:, res_order], 0, nAssets, np.ones(nAssets))
    W_ret = np.empty(nAssets)
    W_ret[res_order] = W_sort
    return W_ret

def HRP(cov, corr, isGPU = True):
    if isGPU:
        return HRP_GPU(cov, corr)
    else:
        return HRP_CPU(cov, corr)