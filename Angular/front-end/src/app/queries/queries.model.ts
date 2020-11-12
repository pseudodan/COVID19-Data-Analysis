// Create the Query class that holds the name and description of a query
export class Query
{
  public name: string;
  public description: string;

  constructor(name: string, description: string)
  {
    this.name = name;
    this.description = description;
  }
}
