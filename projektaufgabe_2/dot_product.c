#include "postgres.h"
#include "utils/array.h"
#include "fmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(dot_product_c);

Datum
dot_product_c(PG_FUNCTION_ARGS)
{
    ArrayType *v1 = PG_GETARG_ARRAYTYPE_P(0);
    ArrayType *v2 = PG_GETARG_ARRAYTYPE_P(1);

    int n1 = ArrayGetNItems(ARR_NDIM(v1), ARR_DIMS(v1));
    int n2 = ArrayGetNItems(ARR_NDIM(v2), ARR_DIMS(v2));
    
    if (n1 != n2) PG_RETURN_FLOAT8(0.0);

    float8 *d1 = (float8 *) ARR_DATA_PTR(v1);
    float8 *d2 = (float8 *) ARR_DATA_PTR(v2);

    float8 sum = 0.0;
    for (int i = 0; i < n1; i++) {
        sum += d1[i] * d2[i];
    }

    PG_RETURN_FLOAT8(sum);
}

//cc -fPIC -I$(pg_config --includedir-server) -shared -o dot_product.so dot_product.c