import requests
import time

def query(params, service_url, attempts=5):
    
    last = None

    for attempt in range(attempts):

        try:

            r = requests.get(
                f"{service_url}/query",
                params={**params, "f": "json"},
                timeout=60,
            )

            r.raise_for_status()

            body = r.json()

            if "error" in body:

                code = body["error"].get("code")

                if code in (400, 403, 404):
                    raise ValueError(f"ArcGIS rejected the query ({code}): {body['error']}")
                
                raise RuntimeError(f"ArcGIS error: {body['error']}")
            
            return body

        except ValueError as exc:
            raise exc
        except Exception as exc:

            last = exc
            wait = 2 ** attempt

            print(f"[query] attempt {attempt + 1} failed: {exc}; retrying in {wait}s", flush=True)

            time.sleep(wait)

    raise last