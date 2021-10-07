use lambda::{handler_fn, Context};
use serde_json::{Value, json};
use async_std;

use aws_config::meta::region::RegionProviderChain;
use aws_sdk_sns::{Client, Region, PKG_VERSION};

type Error = Box<dyn std::error::Error + Sync + Send + 'static>;

#[async_std::main]
async fn main() -> Result<(), Error> {
    lambda::run(handler_fn(handler)).await?;
    Ok(())
}

async fn handler(event: Value, _: Context) -> Result<Value, Error> {
    let region = Some(String::from(event["Records"][0]["awsRegion"].as_str().unwrap()));
    let bucket = event["Records"][0]["s3"]["bucket"]["name"].as_str().unwrap();
    let key = event["Records"][0]["s3"]["object"]["key"].as_str().unwrap();

    let topic_arn:String;

    let email = String::from("charithr007@gmail.com");

    let region_provider = RegionProviderChain::first_try(region.clone().map(Region::new))
        .or_default_provider()
        .or_else("us-east-2");
    
    let shared_config = aws_config::from_env().region(region_provider).load().await;

    let client = Client::new(&shared_config);
    
    println!("\nCreating an SNS topic...");

    let response = client
        .create_topic()
        .name("BucketCopySNS")
        .send()
        .await?;
    
    topic_arn = response.topic_arn.unwrap_or_default();

    println!("\nSubscribing to the SNS topic...");

    let response = client
        .subscribe()
        .topic_arn(&topic_arn)
        .protocol("email")
        .endpoint(&email)
        .send()
        .await?;
    
    if let Some(_arn) = response.subscription_arn{
        println!("\nSuccessfully subscribed to email: {}", &email);

        let response = client
            .publish()
            .topic_arn(&topic_arn)
            .message(format!("File: {} is copied to your s3 bucket: {} in region: {}", &key, &bucket, &region.as_ref().unwrap()))
            .send()
            .await?;
        
        if let Some(_) = response.message_id{
            println!("\nMessage successful sent!");
        }
    }

    let json_result = json!({
        "region": region.as_ref().unwrap(),
        "bucket": bucket,
        "file": key,
        "Notification": format!("Email notificaion sent to: {}", &email),
    });
    Ok(json_result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[async_std::test]
    async fn handler_handles() {
        let event = json!({
            "answer": 42
        });
        assert_eq!(
            handler(event.clone(), Context::default())
                .await
                .expect("expected Ok(_) value"),
            event
        )
    }
}