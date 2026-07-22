import { getDeployStore } from "https://esm.sh/@netlify/blobs@10.7.9";
let _resultStore: ReturnType<typeof getDeployStore> | undefined;

export const blobStores = {
    get requestResults() {
        return _resultStore ??= getDeployStore({
            name: "request-results",
            consistency: 'eventual'
        });
    }
};