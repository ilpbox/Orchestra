interface result {
	query: string;
	values: any[];
	original(): any;
}
interface mongosql {
	sql(query: any, values?: any[]): result;
	toString(): string;
	toQuery(): result;
}