package org.apache.atlas.auth.client.heracles;

import org.apache.atlas.auth.client.heracles.models.HeraclesGroupViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesGroupsResponse;
import org.apache.atlas.auth.client.heracles.models.HeraclesRoleViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.HeraclesUserViewRepresentation;
import org.apache.atlas.auth.client.heracles.models.IdentityGroupRepresentation;
import org.apache.atlas.auth.client.heracles.models.IdentityRoleRepresentation;
import org.apache.atlas.auth.client.heracles.models.IdentityUserRepresentation;
import retrofit2.Call;
import retrofit2.http.GET;
import retrofit2.http.Headers;
import retrofit2.http.Query;

import java.util.List;

public interface RetrofitHeraclesClient {
    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/users/mappings")
    Call<List<HeraclesUserViewRepresentation>> getUsersMapping(@Query("offset") Integer offset, @Query("limit") Integer limit, @Query("sort") String sort,
                                                               @Query("columns") String[] columns);

    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/roles/mappings")
    Call<List<HeraclesRoleViewRepresentation>> getRolesMapping(@Query("offset") Integer offset, @Query("limit") Integer limit, @Query("sort") String sort,
                                                               @Query("columns") String[] columns);

    /**
     * List groups from Heracles API (v2) with relation lookups
     * API returns a wrapped response with records array
     * 
     * @param offset Offset for pagination
     * @param limit The numbers of items to return  
     * @param sort Column names for sorting (+/-)
     * @param columns Column names to project
     * @param filter Filter string
     * @param count Whether to process count
     * @param relations Column names to lookup
     * @return Wrapped response containing list of groups
     */
    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/v2/groups")
    Call<HeraclesGroupsResponse> getGroupsV2(@Query("offset") Integer offset, 
                                             @Query("limit") Integer limit, 
                                             @Query("sort") String[] sort,
                                             @Query("columns") String[] columns,
                                             @Query("filter") String filter,
                                             @Query("count") Boolean count,
                                             @Query("relations") String[] relations);

    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/identity/users")
    Call<List<IdentityUserRepresentation>> getIdentityUsers(@Query("filter") String filter, @Query("columns") String... columns);

    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/identity/roles")
    Call<List<IdentityRoleRepresentation>> getIdentityRoles(@Query("filter") String filter, @Query("columns") String... columns);

    @Headers({"Accept: application/json,text/plain", "Cache-Control: no-store", "Cache-Control: no-cache"})
    @GET("/identity/groups")
    Call<List<IdentityGroupRepresentation>> getIdentityGroups(@Query("filter") String filter, @Query("columns") String... columns);
}
