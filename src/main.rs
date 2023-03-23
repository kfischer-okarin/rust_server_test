use r2d2::{ManageConnection, Pool};
use warp::hyper::body::Bytes;
use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::{Arc, Mutex};
use warp::Filter;

// A type alias for the in-memory datastructure
type Data = Arc<Mutex<HashMap<String, String>>>;

// A struct that implements ManageConnection for Data
struct DataManager {
    data: Data,
}

impl DataManager {
    fn new() -> Self {
        DataManager {
            data: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Debug)]
enum ConnectionError {
    LockError,
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ConnectionError::LockError => write!(f, "LockError"),
        }
    }
}

impl std::error::Error for ConnectionError {}

impl ManageConnection for DataManager {
    type Connection = Data;
    type Error = ConnectionError;

    // Create a new connection by creating an empty HashMap
    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        Ok(self.data.clone())
    }

    // Check if the connection is valid by trying to lock it
    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        match conn.lock() {
            Ok(_) => Ok(()),
            Err(_e) => Err(ConnectionError::LockError),
        }
    }

    // Do nothing when the connection is recycled
    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

// A function that creates a connection pool
fn create_pool() -> Pool<DataManager> {
    // Create a pool using the custom manager
    Pool::builder()
        .max_size(10)
        .build(DataManager::new())
        .unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Create a connection pool
    let pool = create_pool();
    // Clone the pool for each request
    let with_pool = |pool: Pool<DataManager>| warp::any().map(move || pool.clone());

    let get_data = warp::path("data")
        .and(warp::get())
        .and(warp::path::param())
        .and(with_pool(pool.clone()))
        .and_then(|id: String, pool: Pool<DataManager>| async move {
            // Get a connection from the pool
            let conn = pool.get().unwrap();
            // Lock the connection
            let data = conn.lock().unwrap();
            // Get the user from the connection
            let value = data.get(&id).unwrap();

            let result: Result<String, Infallible> = Ok(value.clone());
            result
        });
    let set_data = warp::path("data")
        .and(warp::path::param())
        .and(warp::body::bytes())
        .and(with_pool(pool))
        .and(warp::put())
        .and_then(|id: String, body: Bytes, pool: Pool<DataManager>| async move {
            // Get a connection from the pool
            let conn = pool.get().unwrap();
            // Lock the connection
            let mut data = conn.lock().unwrap();
            let value = String::from_utf8(body.to_vec()).unwrap();
            println!("Setting data: {} = {}", id, value);
            // Set the user in the connection
            data.insert(id, value.clone());
            // Return the user
            let result: Result<String, Infallible> = Ok(value.clone());
            result
        });

    warp::serve(get_data.or(set_data))
        .run(([127, 0, 0, 1], 3030))
        .await;
}
