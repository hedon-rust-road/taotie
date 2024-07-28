use std::ops::Deref;

use arrow_cast::pretty::pretty_format_batches;
use datafusion::prelude::{SessionConfig, SessionContext};

use crate::{cli::DatasetConn, Backend, ReplDisplay};

pub struct DataFusionBackend(SessionContext);

impl DataFusionBackend {
    pub fn new() -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().catalog.information_schema = true;
        let ctx = SessionContext::new_with_config(config);
        Self(ctx)
    }
}

impl Backend for DataFusionBackend {
    type DataFrame = datafusion::dataframe::DataFrame;
    async fn connect(&mut self, opts: &crate::cli::ConnectOpts) -> anyhow::Result<()> {
        match &opts.conn {
            DatasetConn::Postgres(_conn_str) => {
                println!("Postgres is not supported yet")
            }
            DatasetConn::Csv(filename) => {
                self.register_csv(&opts.name, filename, Default::default())
                    .await?;
            }
            DatasetConn::Parquet(filename) => {
                self.register_parquet(&opts.name, filename, Default::default())
                    .await?;
            }
            DatasetConn::NdJson(filename) => {
                self.register_json(&opts.name, filename, Default::default())
                    .await?;
            }
        }
        Ok(())
    }
    async fn list(&self) -> anyhow::Result<Self::DataFrame> {
        let sql = "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = 'public'";
        let df = self.0.sql(sql).await?;
        Ok(df)
    }
    async fn schema(&self, name: &str) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(&format!("DESCRIBE {}", name)).await?;
        Ok(df)
    }
    async fn describe(&self, name: &str) -> anyhow::Result<Self::DataFrame> {
        let df = self.0.sql(&format!("SELECT * FROM {}", name)).await?;
        Ok(df)
    }
    async fn head(&self, name: &str, size: usize) -> anyhow::Result<Self::DataFrame> {
        let df = self
            .0
            .sql(&format!("SELECT * FROM {} LIMIT {}", name, size))
            .await?;
        Ok(df)
    }

    async fn sql(&self, sql: &str) -> anyhow::Result<Self::DataFrame> {
        // Why can not here use self.sql?
        // recursion in an async fn requires boxing a recursive `async fn` call must introduce indirection such as `Box::pin` to avoid an infinitely sized future
        /*
         Rust 的编译器在处理 async fn 时，会为每个异步函数生成一个状态机。
         这个状态机会保存函数的局部变量以及其执行的当前状态。
         当一个异步函数调用自己时，编译器会尝试在当前状态机中嵌套另一个同样类型的状态机，这会导致状态机的大小无限增长，
         因为每次递归调用都会创建一个新的状态机实例，而这些状态机实例都需要在堆栈上展开。
         报错信息提示在异步函数中存在递归调用，而这种递归调用需要引入间接性来避免生成无限大小的未来对象。
         具体来说，报错信息提到需要使用 Box::pin 来进行装箱，以避免无限大小的 future。

        self.0.sql:
            这种情况假设 self 是一个包含另一个对象的结构体或元组，并且你调用的是这个内部对象的 sql 方法。
            在这种情况下，self.0.sql 调用的是 self 的某个字段的 sql 方法，不涉及当前类型的方法调用，不会引起递归问题。

        self.sql:
            这种情况下调用的是当前类型的 sql 方法，即你正在定义的异步函数。
            由于 Rust 生成的状态机在处理递归时会导致无限大小的 future，所以编译器会报错，要求你装箱以避免这种情况。

        哈哈，所以这里核心原因其实就是 struct 和它的 inner 都拥有一个同名的方法！
        */
        let df = self.0.sql(sql).await?;
        Ok(df)
    }
}

impl Default for DataFusionBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for DataFusionBackend {
    type Target = SessionContext;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReplDisplay for datafusion::dataframe::DataFrame {
    async fn display(self) -> anyhow::Result<String> {
        let batches = self.collect().await?;
        let data = pretty_format_batches(&batches)?;
        Ok(data.to_string())
    }
}
